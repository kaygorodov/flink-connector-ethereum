package org.kaigorodov.flink.connector.ethereum;

public class EthereumBlockWithCheckInfo {
  private EthBlock ethBlock;

  public EthereumBlockWithCheckInfo(EthBlock ethBlock) {
    this.ethBlock = ethBlock;
  }

  public EthBlock getEthBlock() {
    return ethBlock;
  }
}
