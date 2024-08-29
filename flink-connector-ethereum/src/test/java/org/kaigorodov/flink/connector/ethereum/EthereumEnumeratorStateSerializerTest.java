package org.kaigorodov.flink.connector.ethereum;

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