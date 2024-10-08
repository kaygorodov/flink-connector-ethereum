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
package io.github.kaygorodov.flink.connector.ethereum.client;

import java.io.IOException;
import java.math.BigInteger;
import io.github.kaygorodov.flink.connector.ethereum.model.EthBlock;
import org.apache.flink.annotation.Internal;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.http.HttpService;

@Internal
public class EthNetworkClient {
  final private Web3j web3;

  public EthNetworkClient(String URL) {
    this.web3 = Web3j.build(new HttpService(URL));
  }

  public BigInteger getLatestBlockNumber() {
    try {
      return web3.ethBlockNumber().send().getBlockNumber();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  @Override
  public String toString() {
    return "EthNetworkClient{" +
        "web3=" + web3 +
        '}';
  }
}
