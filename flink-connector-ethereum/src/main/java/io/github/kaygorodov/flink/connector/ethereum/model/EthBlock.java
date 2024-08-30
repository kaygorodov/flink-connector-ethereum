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
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.Transaction;

public class EthBlock {
  private BigInteger number;
  private String hash;
  private String parentHash;
  private String parentBeaconBlockRoot;
  private BigInteger nonce;
  private String sha3Uncles;
  private String logsBloom;
  private String transactionsRoot;
  private String stateRoot;
  private String receiptsRoot;
  private String author;
  private String miner;
  private String mixHash;
  private BigInteger difficulty;
  private BigInteger totalDifficulty;
  private String extraData;
  private BigInteger size;
  private BigInteger gasLimit;
  private BigInteger gasUsed;
  private BigInteger timestamp;
//  private List<String> uncles;
//  private List<String> sealFields;
  private BigInteger baseFeePerGas;
  private String withdrawalsRoot;
  private BigInteger blobGasUsed;
  private BigInteger excessBlobGas;

  @TypeInfo(ListTypeInfoFactory.class)
  private List<EthTransaction> transactions;

  public EthBlock() {
  }

  public EthBlock(Block rawBlock) {
    this.number = rawBlock.getNumber();
    this.hash = rawBlock.getHash();
    this.parentHash = rawBlock.getParentHash();
    this.parentBeaconBlockRoot = rawBlock.getParentBeaconBlockRoot();
    this.nonce = rawBlock.getNonce();
    this.sha3Uncles = rawBlock.getSha3Uncles();
    this.logsBloom = rawBlock.getLogsBloom();
    this.transactionsRoot = rawBlock.getTransactionsRoot();
    this.stateRoot = rawBlock.getStateRoot();
    this.receiptsRoot = rawBlock.getReceiptsRoot();
    this.author = rawBlock.getAuthor();
    this.miner = rawBlock.getMiner();
    this.mixHash = rawBlock.getMixHash();
    this.difficulty = rawBlock.getDifficulty();
    this.totalDifficulty = rawBlock.getTotalDifficulty();
    this.extraData = rawBlock.getExtraData();
    this.size = rawBlock.getSize();
    this.gasLimit = rawBlock.getGasLimit();
    this.gasUsed = rawBlock.getGasUsed();
    this.timestamp = rawBlock.getTimestamp();
    this.transactions = rawBlock.getTransactions().stream()
            .map(transaction ->
              new EthTransaction((Transaction) transaction.get())
            ).toList();

//    this.uncles = rawBlock.getUncles();
//    this.sealFields = rawBlock.getSealFields();
    this.baseFeePerGas = rawBlock.getBaseFeePerGas();
    this.withdrawalsRoot = rawBlock.getWithdrawalsRoot();
    this.blobGasUsed = rawBlock.getBlobGasUsed();
    this.excessBlobGas = rawBlock.getExcessBlobGas();
  }

  public BigInteger getNumber() {
    return number;
  }

  public void setNumber(BigInteger number) {
    this.number = number;
  }

  public String getHash() {
    return hash;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  public String getParentHash() {
    return parentHash;
  }

  public void setParentHash(String parentHash) {
    this.parentHash = parentHash;
  }

  public String getParentBeaconBlockRoot() {
    return parentBeaconBlockRoot;
  }

  public void setParentBeaconBlockRoot(String parentBeaconBlockRoot) {
    this.parentBeaconBlockRoot = parentBeaconBlockRoot;
  }

  public BigInteger getNonce() {
    return nonce;
  }

  public void setNonce(BigInteger nonce) {
    this.nonce = nonce;
  }

  public String getSha3Uncles() {
    return sha3Uncles;
  }

  public void setSha3Uncles(String sha3Uncles) {
    this.sha3Uncles = sha3Uncles;
  }

  public String getLogsBloom() {
    return logsBloom;
  }

  public void setLogsBloom(String logsBloom) {
    this.logsBloom = logsBloom;
  }

  public String getTransactionsRoot() {
    return transactionsRoot;
  }

  public void setTransactionsRoot(String transactionsRoot) {
    this.transactionsRoot = transactionsRoot;
  }

  public String getStateRoot() {
    return stateRoot;
  }

  public void setStateRoot(String stateRoot) {
    this.stateRoot = stateRoot;
  }

  public String getReceiptsRoot() {
    return receiptsRoot;
  }

  public void setReceiptsRoot(String receiptsRoot) {
    this.receiptsRoot = receiptsRoot;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getMiner() {
    return miner;
  }

  public void setMiner(String miner) {
    this.miner = miner;
  }

  public String getMixHash() {
    return mixHash;
  }

  public void setMixHash(String mixHash) {
    this.mixHash = mixHash;
  }

  public BigInteger getDifficulty() {
    return difficulty;
  }

  public void setDifficulty(BigInteger difficulty) {
    this.difficulty = difficulty;
  }

  public BigInteger getTotalDifficulty() {
    return totalDifficulty;
  }

  public void setTotalDifficulty(BigInteger totalDifficulty) {
    this.totalDifficulty = totalDifficulty;
  }

  public String getExtraData() {
    return extraData;
  }

  public void setExtraData(String extraData) {
    this.extraData = extraData;
  }

  public BigInteger getSize() {
    return size;
  }

  public void setSize(BigInteger size) {
    this.size = size;
  }

  public BigInteger getGasLimit() {
    return gasLimit;
  }

  public void setGasLimit(BigInteger gasLimit) {
    this.gasLimit = gasLimit;
  }

  public BigInteger getGasUsed() {
    return gasUsed;
  }

  public void setGasUsed(BigInteger gasUsed) {
    this.gasUsed = gasUsed;
  }

  public BigInteger getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(BigInteger timestamp) {
    this.timestamp = timestamp;
  }

//  public List<String> getUncles() {
//    return uncles;
//  }
//
//  public void setUncles(List<String> uncles) {
//    this.uncles = uncles;
//  }
//
//  public List<String> getSealFields() {
//    return sealFields;
//  }
//
//  public void setSealFields(List<String> sealFields) {
//    this.sealFields = sealFields;
//  }

  public BigInteger getBaseFeePerGas() {
    return baseFeePerGas;
  }

  public void setBaseFeePerGas(BigInteger baseFeePerGas) {
    this.baseFeePerGas = baseFeePerGas;
  }

  public String getWithdrawalsRoot() {
    return withdrawalsRoot;
  }

  public void setWithdrawalsRoot(String withdrawalsRoot) {
    this.withdrawalsRoot = withdrawalsRoot;
  }

  public BigInteger getBlobGasUsed() {
    return blobGasUsed;
  }

  public void setBlobGasUsed(BigInteger blobGasUsed) {
    this.blobGasUsed = blobGasUsed;
  }

  public BigInteger getExcessBlobGas() {
    return excessBlobGas;
  }

  public void setExcessBlobGas(BigInteger excessBlobGas) {
    this.excessBlobGas = excessBlobGas;
  }

  @Override
  public String toString() {
    return "EthBlock{" + "\n" +
        "  number=" + number + ",\n" +
        "  hash='" + hash + '\'' + ",\n" +
        "  parentHash='" + parentHash + '\'' + ",\n" +
        "  parentBeaconBlockRoot='" + parentBeaconBlockRoot + '\'' + ",\n" +
        "  nonce=" + nonce + ",\n" +
        "  sha3Uncles='" + sha3Uncles + '\'' + ",\n" +
        "  logsBloom='" + logsBloom + '\'' + ",\n" +
        "  transactionsRoot='" + transactionsRoot + '\'' + ",\n" +
        "  stateRoot='" + stateRoot + '\'' + ",\n" +
        "  receiptsRoot='" + receiptsRoot + '\'' + ",\n" +
        "  author='" + author + '\'' + ",\n" +
        "  miner='" + miner + '\'' + ",\n" +
        "  mixHash='" + mixHash + '\'' + ",\n" +
        "  difficulty=" + difficulty + ",\n" +
        "  totalDifficulty=" + totalDifficulty + ",\n" +
        "  extraData='" + extraData + '\'' + ",\n" +
        "  size=" + size + ",\n" +
        "  gasLimit=" + gasLimit + ",\n" +
        "  gasUsed=" + gasUsed + ",\n" +
        "  timestamp=" + timestamp + ",\n" +
        "  baseFeePerGas=" + baseFeePerGas + ",\n" +
        "  withdrawalsRoot='" + withdrawalsRoot + '\'' + ",\n" +
        "  blobGasUsed=" + blobGasUsed + ",\n" +
        "  excessBlobGas=" + excessBlobGas + "\n" +
        "  transactions=" + transactions + "\n" +
        '}';
  }

  public List<EthTransaction> getTransactions() {
    return transactions;
  }

  public void setTransactions(List<EthTransaction> transactions) {
    this.transactions = transactions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EthBlock ethBlock = (EthBlock) o;
    return Objects.equals(number, ethBlock.number) && Objects.equals(hash,
        ethBlock.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(number, hash);
  }
}
