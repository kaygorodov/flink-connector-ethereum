/*
 * Copyright © 2024 Andrei Kaigorodov (andreykaygorodov@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.kaygorodov.flink.connector.ethereum;

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
