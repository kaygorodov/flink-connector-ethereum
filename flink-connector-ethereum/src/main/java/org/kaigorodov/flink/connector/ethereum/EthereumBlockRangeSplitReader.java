package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EthereumBlockRangeSplitReader implements SplitReader<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit> {

    private static class EthereumBlocksWithRangeSplits implements RecordsWithSplitIds<EthereumBlockWithCheckInfo> {
        private List<EthereumBlockWithCheckInfo> records;
        private Iterator<EthereumBlockWithCheckInfo> recordIterator;

        public EthereumBlocksWithRangeSplits(List<EthereumBlockWithCheckInfo> records) {
            this.records = records;
            this.recordIterator = records.iterator();
        }

        @Nullable
        @Override
        public String nextSplit() {
            return "123123123";
        }

        @Nullable
        @Override
        public EthereumBlockWithCheckInfo nextRecordFromSplit() {
            return recordIterator.next();
        }

        @Override
        public Set<String> finishedSplits() {
            return null;
        }
    }
    @Override
    public RecordsWithSplitIds<EthereumBlockWithCheckInfo> fetch() throws IOException {
        var records = List.of(
            new EthereumBlockWithCheckInfo(),
            new EthereumBlockWithCheckInfo(),
            new EthereumBlockWithCheckInfo(),
            new EthereumBlockWithCheckInfo()
        );

        return new EthereumBlocksWithRangeSplits(records);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<EthereumBlockRangeSplit> splitsChanges) {

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
