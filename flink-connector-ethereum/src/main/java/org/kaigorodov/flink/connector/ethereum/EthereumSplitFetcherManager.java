package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.function.Supplier;

public class EthereumSplitFetcherManager extends SingleThreadFetcherManager<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit> {
    public EthereumSplitFetcherManager(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<EthereumBlockWithCheckInfo>> elementsQueue,
        Supplier<SplitReader<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier, new Configuration());
    }
}
