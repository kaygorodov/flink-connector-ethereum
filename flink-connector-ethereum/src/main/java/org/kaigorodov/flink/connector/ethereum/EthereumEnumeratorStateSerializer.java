package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class EthereumEnumeratorStateSerializer implements SimpleVersionedSerializer<EthereumEnumeratorState> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(EthereumEnumeratorState obj) throws IOException {
        return new byte[0];
    }

    @Override
    public EthereumEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        return new EthereumEnumeratorState();
    }
}
