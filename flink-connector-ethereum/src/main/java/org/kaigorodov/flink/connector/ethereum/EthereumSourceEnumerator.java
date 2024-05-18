package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class EthereumSourceEnumerator implements SplitEnumerator<EthereumBlockSplit, EthereumEnumeratorState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumSourceEnumerator.class);

    private String ethNodeUrl;
    private Web3j web3jClient;
    private BigInteger initialBlockNumber;


    public EthereumSourceEnumerator(BigInteger initialBlockNumber, String ethNodeUrl) {
        this.initialBlockNumber = initialBlockNumber;
        this.ethNodeUrl = ethNodeUrl;
    }

    @Override
    public void start() {
        logger.info("Starting EthereumSourceEnumerator using Node URL: {}", ethNodeUrl);
        web3jClient = Web3j.build(new HttpService(ethNodeUrl));
        if (initialBlockNumber == null) {
            logger.info("Initial Block Number is not set. Fetching the latest number from the network");
            try {
                initialBlockNumber = web3jClient.ethBlockNumber().send().getBlockNumber();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("Initial Block Number is {}", initialBlockNumber);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<EthereumBlockSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public EthereumEnumeratorState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
