package org.kaigorodov.flink.connector.ethereum;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.slf4j.LoggerFactory;


public class EthereumBlockRangeSplitReader implements SplitReader<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit> {

    final private SourceReaderContext readerContext;

    public EthereumBlockRangeSplitReader(SourceReaderContext readerContext) {
        this.readerContext = readerContext;
    }

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(EthereumBlockRangeSplitReader.class);

    private Deque<EthereumBlockRangeSplit> splitsToProcess = new ArrayDeque<>();

    private static class EthereumBlocksWithRangeSplits implements RecordsWithSplitIds<EthereumBlockWithCheckInfo> {
        private EthereumBlockRangeSplit split;
        private Set<String> finishedSplits = new HashSet<>();
        private List<BigInteger> blockIds;
        private Iterator<BigInteger> blockIdIterator;

        public EthereumBlocksWithRangeSplits(EthereumBlockRangeSplit split) {
            this.split = split;
            if (split == null) {
                this.blockIds = List.of();
            } else {
                this.blockIds = split.getBlockIds();
            }
            this.blockIdIterator = blockIds.iterator();
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (blockIdIterator.hasNext()) {
                return split.splitId();
            } else {
                return null;
            }
        }

        @Nullable
        @Override
        public EthereumBlockWithCheckInfo nextRecordFromSplit() {
            if (blockIdIterator.hasNext()) {
                return new EthereumBlockWithCheckInfo(
                    new EthBlock(blockIdIterator.next().longValue())
                );
            } else {
                finishedSplits.add(split.splitId());
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
            return new EthereumBlocksWithRangeSplits(null);
        } else {
            askedForSplit = false;
            return new EthereumBlocksWithRangeSplits(splitsToProcess.removeFirst());
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
//        sr.notifyToWakeUp();
    }

    @Override
    public void close() throws Exception {

    }
}
