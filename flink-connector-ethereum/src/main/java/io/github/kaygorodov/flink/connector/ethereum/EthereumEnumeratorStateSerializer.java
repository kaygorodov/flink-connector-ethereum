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
