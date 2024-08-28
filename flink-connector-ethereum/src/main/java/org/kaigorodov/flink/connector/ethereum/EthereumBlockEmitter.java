package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.kaigorodov.flink.connector.ethereum.model.EthBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EthereumBlockEmitter implements RecordEmitter<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockRangeSplitState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumBlockEmitter.class);

    @Override
    public void emitRecord(EthereumBlockWithCheckInfo element, SourceOutput<EthBlock> output, EthereumBlockRangeSplitState splitState) throws Exception {
        logger.info("Emitting record {}", element.getEthBlock());
        output.collect(
            element.getEthBlock()
        );

    }
}
