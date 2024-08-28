package org.kaigorodov.flink.connector.ethereum;

import org.kaigorodov.flink.connector.ethereum.model.EthBlock;

public class EthereumBlockWithCheckInfo {
  private EthBlock ethBlock;

  public EthereumBlockWithCheckInfo(EthBlock ethBlock) {
    this.ethBlock = ethBlock;
  }

  public EthBlock getEthBlock() {
    return ethBlock;
  }
}
