package org.kaigorodov.flink.connector.ethereum;

import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

public class EthereumSourceReader extends
    SourceReaderBase<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockRangeSplit, EthereumBlockRangeSplitState> {


    public EthereumSourceReader(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<EthereumBlockWithCheckInfo>> elementsQueue,
        SingleThreadFetcherManager<EthereumBlockWithCheckInfo, EthereumBlockRangeSplit> splitFetcherManager,
        RecordEmitter<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockRangeSplitState> recordEmitter,
        Configuration config, SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, EthereumBlockRangeSplitState> finishedSplitIds) {

    }

    @Override
    protected EthereumBlockRangeSplitState initializedState(EthereumBlockRangeSplit split) {
        return new EthereumBlockRangeSplitState();
    }

    @Override
    protected EthereumBlockRangeSplit toSplitType(String splitId, EthereumBlockRangeSplitState splitState) {
        return new EthereumBlockRangeSplit(splitId);
    }
}
