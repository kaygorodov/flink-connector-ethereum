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

import io.github.kaygorodov.flink.connector.ethereum.EthereumEnumeratorState;
import io.github.kaygorodov.flink.connector.ethereum.EthereumEnumeratorStateSerializer;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.flink.core.io.VersionMismatchException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EthereumEnumeratorStateSerializerTest {

  @Test
  void shouldSerializeAndDeserializeState() throws IOException {
    var state = new EthereumEnumeratorState(BigInteger.valueOf(13241l));

    var serializer = new EthereumEnumeratorStateSerializer();

    var bytes = serializer.serialize(state);
    var result = serializer.deserialize(serializer.getVersion(), bytes);

    Assertions.assertEquals(state.getLastAssignedBlockNumber(), result.getLastAssignedBlockNumber());
  }

  @Test
  void shouldThrowIfSerializedObjDoesntMatchVersion() throws IOException {
    var serializer = new EthereumEnumeratorStateSerializer();
    Assertions.assertThrows(VersionMismatchException.class, () -> {
      serializer.deserialize(serializer.getVersion() + 1, new byte[]{});
    });
  }

}