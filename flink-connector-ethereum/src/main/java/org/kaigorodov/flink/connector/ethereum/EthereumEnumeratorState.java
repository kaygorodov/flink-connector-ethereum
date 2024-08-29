package org.kaigorodov.flink.connector.ethereum;

import java.math.BigInteger;

public class EthereumEnumeratorState {
  private final BigInteger lastAssignedBlockNumber;

  public BigInteger getLastAssignedBlockNumber() {
    return lastAssignedBlockNumber;
  }

  public EthereumEnumeratorState(BigInteger lastAssignedBlockNumber) {
    this.lastAssignedBlockNumber = lastAssignedBlockNumber;
  }
}
