package org.kaigorodov.flink.connector.ethereum;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Version 0 - A simplified implementation via serialized Strings, to be improved and optimized (logic for version 0
 * should stay for backward compatibility)
 */
public class EthereumBlockRangeSplitSerializer implements SimpleVersionedSerializer<EthereumBlockRangeSplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(EthereumBlockRangeSplit obj) throws IOException {
        return obj.splitId().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public EthereumBlockRangeSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != 0) {
            throw new IllegalArgumentException(String.format("Unsupported version of the serialized object %d, only " +
                "version 0 is supported", version));
        }
        String splitId = new String(serialized, StandardCharsets.UTF_8);
        return new EthereumBlockRangeSplit(splitId);
    }
}
