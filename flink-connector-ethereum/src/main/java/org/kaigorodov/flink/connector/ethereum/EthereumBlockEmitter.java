package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class EthereumBlockEmitter implements RecordEmitter<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockRangeSplitState> {
    @Override
    public void emitRecord(EthereumBlockWithCheckInfo element, SourceOutput<EthBlock> output, EthereumBlockRangeSplitState splitState) throws Exception {
        output.collect(
            new EthBlock()
        );

    }
}
