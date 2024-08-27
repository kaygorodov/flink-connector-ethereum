package org.kaigorodov.flink.connector.ethereum;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.LoggerFactory;


public class EthereumBlockRangeSplitReader implements SplitReader<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit> {

    final private SourceReaderContext readerContext;
    final private EthNetworkClient ethNetworkClient;

    public EthereumBlockRangeSplitReader(SourceReaderContext readerContext) {
        this.ethNetworkClient = new EthNetworkClient();
        this.readerContext = readerContext;
    }

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(EthereumBlockRangeSplitReader.class);

    private Deque<EthereumBlockRangeSplit> splitsToProcess = new ArrayDeque<>();

    private static class EthereumBlocksWithRangeSplits implements RecordsWithSplitIds<EthereumBlockWithCheckInfo> {
        private String splitId;
        private List<EthBlock> ethBlocks;
        private Iterator<EthBlock> ethBlockIterator;
        private Set<String> finishedSplits;

        public EthereumBlocksWithRangeSplits(List<EthBlock> ethBlocks, String splitId) {
            this.splitId = splitId;
            this.ethBlocks = ethBlocks;
            this.ethBlockIterator = ethBlocks.iterator();
            this.finishedSplits = new HashSet<>();
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (ethBlockIterator.hasNext()) {
                return splitId;
            } else {
                return null;
            }
        }

        @Nullable
        @Override
        public EthereumBlockWithCheckInfo nextRecordFromSplit() {
            if (ethBlockIterator.hasNext()) {
                return new EthereumBlockWithCheckInfo(
                    ethBlockIterator.next()
                );
            } else {
                finishedSplits.add(this.splitId);
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }

    boolean askedForSplit = false;


    @Override
    public RecordsWithSplitIds<EthereumBlockWithCheckInfo> fetch() throws IOException {
      if(splitsToProcess.isEmpty()) {
            if (!askedForSplit) {
                readerContext.sendSplitRequest();
                askedForSplit = true;
            }
            return new EthereumBlocksWithRangeSplits(List.of(), null);
        } else {
            askedForSplit = false;

            var split = splitsToProcess.removeFirst();
            var ethBlocks = split.getBlockIds().stream().map(
                ethNetworkClient::getBlockByNumber
            ).toList();

            return new EthereumBlocksWithRangeSplits(ethBlocks, split.splitId());
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<EthereumBlockRangeSplit> splitsChanges) {

        logger.info("Split changes: " + splitsChanges.toString());

        if(splitsChanges instanceof SplitsAddition<EthereumBlockRangeSplit> splitsAddition) {
            for(EthereumBlockRangeSplit split: splitsAddition.splits()) {
                splitsToProcess.addLast(split);
            }
        }
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {

    }
}
