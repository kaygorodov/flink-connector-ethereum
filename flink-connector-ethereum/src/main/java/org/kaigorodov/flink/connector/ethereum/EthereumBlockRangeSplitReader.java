package org.kaigorodov.flink.connector.ethereum;

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
import org.kaigorodov.flink.connector.ethereum.model.EthBlock;
import org.slf4j.LoggerFactory;


public class EthereumBlockRangeSplitReader implements SplitReader<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit> {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EthereumBlockRangeSplitReader.class);
    private final static RecordsWithSplitIds<EthereumBlockWithCheckInfo> EMPTY_RECORDS_WITH_SPLIT_IDS = new EthereumBlocksWithRangeSplits(List.of(), null);

    private final SourceReaderContext readerContext;
    private final EthNetworkClient ethNetworkClient;

    private boolean askedForSplit = false;

    public EthereumBlockRangeSplitReader(SourceReaderContext readerContext, EthNetworkClient ethNetworkClient) {
        this.ethNetworkClient = ethNetworkClient;
        this.readerContext = readerContext;
    }

    public EthereumBlockRangeSplitReader(SourceReaderContext readerContext, String url) {
        this(readerContext, new EthNetworkClient(url));
    }

    private final Deque<EthereumBlockRangeSplit> splitsToProcess = new ArrayDeque<>();

    private static class EthereumBlocksWithRangeSplits implements RecordsWithSplitIds<EthereumBlockWithCheckInfo> {
        private final String splitId;
        private final Iterator<EthBlock> ethBlockIterator;
        private final Set<String> finishedSplits;

        public EthereumBlocksWithRangeSplits(List<EthBlock> ethBlocks, String splitId) {
            this.splitId = splitId;
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
                var block = new EthereumBlockWithCheckInfo(
                    ethBlockIterator.next()
                );

                if (!ethBlockIterator.hasNext()) { // eagerly checking here, if true, it means we just finished the split
                    finishedSplits.add(this.splitId);
                }

                return block;
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }

    @Override
    public RecordsWithSplitIds<EthereumBlockWithCheckInfo> fetch() {
      if(splitsToProcess.isEmpty()) {
            if (!askedForSplit) {
                readerContext.sendSplitRequest();
                askedForSplit = true;
            }
            return EMPTY_RECORDS_WITH_SPLIT_IDS;
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
      logger.info("Received SplitChange: {}", splitsChanges.toString());

        if(splitsChanges instanceof SplitsAddition<EthereumBlockRangeSplit> splitsAddition) {
            for(EthereumBlockRangeSplit split: splitsAddition.splits()) {
                splitsToProcess.addLast(split);
            }
        } else {
            logger.info("Only handles addition SplitChanges");
        }
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {

    }
}
