package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.api.connector.source.SourceSplit;

public class EthereumBlockSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
