package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class EthereumSourceEnumerator implements SplitEnumerator<EthereumBlockRangeSplit, EthereumEnumeratorState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumSourceEnumerator.class);

    private String ethNodeUrl;
    private BigInteger initialBlockNumber;
    private BigInteger lastAssignedBlockNumber;
    private SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext;


    public EthereumSourceEnumerator(SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext,
                                    BigInteger initialBlockNumber, String ethNodeUrl) {
        this.enumContext = enumContext;
        this.initialBlockNumber = initialBlockNumber;
        this.ethNodeUrl = ethNodeUrl;
    }

    @Override
    public void start() {
        logger.info("Starting EthereumSourceEnumerator using Node URL: {}", ethNodeUrl);
//        if (initialBlockNumber == null) {
//            logger.info("Initial Block Number is not set. Fetching the latest number from the network");
//            try {
//                initialBlockNumber = web3jClient.ethBlockNumber().send().getBlockNumber();
//
////                EthBlock.Block block = web3jClient.ethGetBlockByNumber().send().getBlock();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
        logger.info("Initial Block Number is {}", initialBlockNumber);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        logger.info("Subtask {} requested a new split", subtaskId);

        if (lastAssignedBlockNumber == null) {
            lastAssignedBlockNumber = initialBlockNumber;
        } else {
            lastAssignedBlockNumber = lastAssignedBlockNumber.add(BigInteger.ONE);
        }

        this.enumContext.assignSplit(new EthereumBlockRangeSplit(List.of(lastAssignedBlockNumber)), subtaskId);

    }

    @Override
    public void addSplitsBack(List<EthereumBlockRangeSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {
        logger.info("add reader request for subtaskid {}", subtaskId);
        this.handleSplitRequest(subtaskId, null);

    }

    @Override
    public EthereumEnumeratorState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
