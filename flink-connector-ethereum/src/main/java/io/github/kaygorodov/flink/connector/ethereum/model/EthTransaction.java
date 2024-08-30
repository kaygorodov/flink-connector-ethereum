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
package io.github.kaygorodov.flink.connector.ethereum.model;

import io.github.kaygorodov.flink.connector.ethereum.serialization.ListTypeInfoFactory;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.web3j.protocol.core.methods.response.Transaction;

public class EthTransaction {
  private String hash;
  private BigInteger nonce;
  private String blockHash;
  private BigInteger blockNumber;
  private long chainId;
  private BigInteger transactionIndex;
  private String from;
  private String to;
  private BigInteger value;
  private BigInteger gasPrice;
  private BigInteger gas;
  private String input;
  private String creates;
  private String publicKey;
  private String raw;
  private String r;
  private String s;
  private long v;
  private String yParity;
  private String type;
  private BigInteger maxFeePerGas;
//  private BigInteger maxPriorityFeePerGas;
//  private BigInteger maxFeePerBlobGas;
  @TypeInfo(ListTypeInfoFactory.class)
  private List<String> blobVersionedHashes;

  public EthTransaction() {
  }

  public EthTransaction(Transaction rawTransaction) {
    this.hash = rawTransaction.getHash();
    this.nonce = rawTransaction.getNonce();
    this.blockHash = rawTransaction.getBlockHash();
    this.blockNumber = rawTransaction.getBlockNumber();
    this.chainId = rawTransaction.getChainId();
    this.transactionIndex = rawTransaction.getTransactionIndex();
    this.from = rawTransaction.getFrom();
    this.to = rawTransaction.getTo();
    this.value = rawTransaction.getValue();
    this.gasPrice = rawTransaction.getGasPrice();
    this.gas = rawTransaction.getGas();
    this.input = rawTransaction.getInput();
    this.creates = rawTransaction.getCreates();
    this.publicKey = rawTransaction.getPublicKey();
    this.raw = rawTransaction.getRaw();
    this.r = rawTransaction.getR();
    this.s = rawTransaction.getS();
    this.v = rawTransaction.getV();
    this.yParity = rawTransaction.getyParity();
    this.type = rawTransaction.getType();
    this.maxFeePerGas = rawTransaction.getMaxFeePerGas();
//    this.maxPriorityFeePerGas = rawTransaction.getMaxPriorityFeePerGas();
//    this.maxFeePerBlobGas = rawTransaction.getMaxFeePerBlobGas();
    this.blobVersionedHashes = rawTransaction.getBlobVersionedHashes();
  }

  public String getHash() {
    return hash;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  public BigInteger getNonce() {
    return nonce;
  }

  public void setNonce(BigInteger nonce) {
    this.nonce = nonce;
  }

  public String getBlockHash() {
    return blockHash;
  }

  public void setBlockHash(String blockHash) {
    this.blockHash = blockHash;
  }

  public BigInteger getBlockNumber() {
    return blockNumber;
  }

  public void setBlockNumber(BigInteger blockNumber) {
    this.blockNumber = blockNumber;
  }

  public long getChainId() {
    return chainId;
  }

  public void setChainId(long chainId) {
    this.chainId = chainId;
  }

  public BigInteger getTransactionIndex() {
    return transactionIndex;
  }

  public void setTransactionIndex(BigInteger transactionIndex) {
    this.transactionIndex = transactionIndex;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public String getTo() {
    return to;
  }

  public void setTo(String to) {
    this.to = to;
  }

  public BigInteger getValue() {
    return value;
  }

  public void setValue(BigInteger value) {
    this.value = value;
  }

  public BigInteger getGasPrice() {
    return gasPrice;
  }

  public void setGasPrice(BigInteger gasPrice) {
    this.gasPrice = gasPrice;
  }

  public BigInteger getGas() {
    return gas;
  }

  public void setGas(BigInteger gas) {
    this.gas = gas;
  }

  public String getInput() {
    return input;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public String getCreates() {
    return creates;
  }

  public void setCreates(String creates) {
    this.creates = creates;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public void setPublicKey(String publicKey) {
    this.publicKey = publicKey;
  }

  public String getRaw() {
    return raw;
  }

  public void setRaw(String raw) {
    this.raw = raw;
  }

  public String getR() {
    return r;
  }

  public void setR(String r) {
    this.r = r;
  }

  public String getS() {
    return s;
  }

  public void setS(String s) {
    this.s = s;
  }

  public long getV() {
    return v;
  }

  public void setV(long v) {
    this.v = v;
  }

  public String getyParity() {
    return yParity;
  }

  public void setyParity(String yParity) {
    this.yParity = yParity;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public BigInteger getMaxFeePerGas() {
    return maxFeePerGas;
  }

  public void setMaxFeePerGas(BigInteger maxFeePerGas) {
    this.maxFeePerGas = maxFeePerGas;
  }

//  public BigInteger getMaxPriorityFeePerGas() {
//    return maxPriorityFeePerGas;
//  }
//
//  public void setMaxPriorityFeePerGas(BigInteger maxPriorityFeePerGas) {
//    this.maxPriorityFeePerGas = maxPriorityFeePerGas;
//  }
//
//  public BigInteger getMaxFeePerBlobGas() {
//    return maxFeePerBlobGas;
//  }
//
//  public void setMaxFeePerBlobGas(BigInteger maxFeePerBlobGas) {
//    this.maxFeePerBlobGas = maxFeePerBlobGas;
//  }

  public List<String> getBlobVersionedHashes() {
    return blobVersionedHashes;
  }

  public void setBlobVersionedHashes(List<String> blobVersionedHashes) {
    this.blobVersionedHashes = blobVersionedHashes;
  }

  @Override
  public String toString() {
    return "EthTransaction{" +
        "hash='" + hash + '\'' +
        ", nonce=" + nonce +
        ", blockHash='" + blockHash + '\'' +
        ", blockNumber=" + blockNumber +
        ", chainId=" + chainId +
        ", transactionIndex=" + transactionIndex +
        ", from='" + from + '\'' +
        ", to='" + to + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EthTransaction that = (EthTransaction) o;
    return Objects.equals(hash, that.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hash);
  }
}
