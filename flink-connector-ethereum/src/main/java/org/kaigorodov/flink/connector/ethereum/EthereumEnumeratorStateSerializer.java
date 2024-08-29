package org.kaigorodov.flink.connector.ethereum;

import java.io.IOException;
import java.math.BigInteger;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

public class EthereumEnumeratorStateSerializer implements SimpleVersionedSerializer<EthereumEnumeratorState> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(EthereumEnumeratorState enumeratorState) throws IOException {
        return enumeratorState.getLastAssignedBlockNumber().toByteArray();
    }

    @Override
    public EthereumEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion()) {
            throw new VersionMismatchException(
                "Trying to deserialize EthereumEnumeratorState serialized with unsupported version "
                    + version
                    + ". Serializer version is "
                    + getVersion());
        }
        return new EthereumEnumeratorState(
            new BigInteger(serialized)
        );
    }
}
