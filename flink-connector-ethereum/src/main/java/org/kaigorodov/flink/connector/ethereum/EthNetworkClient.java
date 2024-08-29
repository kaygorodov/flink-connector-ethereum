package org.kaigorodov.flink.connector.ethereum;

import java.io.IOException;
import java.math.BigInteger;
import org.kaigorodov.flink.connector.ethereum.model.EthBlock;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.http.HttpService;

public class EthNetworkClient {
  final private Web3j web3;

  public EthNetworkClient(String URL) {
    this.web3 = Web3j.build(new HttpService(URL));
  }

  public EthBlock getBlockByNumber(BigInteger blockNumber) {
    final boolean returnFullTransactionObjects = true;
    try {
      var blockResponse = web3.ethGetBlockByNumber(DefaultBlockParameter.valueOf(blockNumber),
          returnFullTransactionObjects).send();
      return new EthBlock(blockResponse.getBlock());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
