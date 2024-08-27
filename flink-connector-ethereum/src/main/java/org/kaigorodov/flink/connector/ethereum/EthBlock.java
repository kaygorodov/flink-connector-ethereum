package org.kaigorodov.flink.connector.ethereum;

public class EthBlock {
  private long blockNumber;

  // Getter and Setter for blockNumber
  public long getBlockNumber() {
    return blockNumber;
  }

  public void setBlockNumber(long blockNumber) {
    this.blockNumber = blockNumber;
  }

  public EthBlock(long blockNumber) {
    this.blockNumber = blockNumber;
  }

  public EthBlock() {}

  @Override
  public String toString() {
    return "EthBlock{" +
        "blockNumber=" + blockNumber +
        '}';
  }
}
