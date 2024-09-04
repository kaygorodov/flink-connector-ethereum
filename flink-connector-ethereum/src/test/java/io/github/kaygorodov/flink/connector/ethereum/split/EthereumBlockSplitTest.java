package io.github.kaygorodov.flink.connector.ethereum.split;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EthereumBlockSplitTest {

  @Test
  void shouldCreateSplitOutOfListOfBlocks() {
    EthereumBlockSplit ethereumBlockSplit = new EthereumBlockSplit(
        List.of(BigInteger.valueOf(777), BigInteger.valueOf(778)));
    assertEquals(ethereumBlockSplit.getBlockIds(), Set.of(BigInteger.valueOf(777), BigInteger.valueOf(778)));
    assertEquals(ethereumBlockSplit.splitId(), "777,778");
  }

  @Test
  void shouldCreateSplitFromOneBlockSplitIdString() {
    EthereumBlockSplit ethereumBlockSplit = new EthereumBlockSplit("777");
    assertEquals(ethereumBlockSplit.getBlockIds(), Set.of(BigInteger.valueOf(777)));
    assertEquals(ethereumBlockSplit.splitId(), "777");
  }

  @Test
  void shouldCreateSplitFromMultipleBlockSplitIdString() {
    EthereumBlockSplit ethereumBlockSplit = new EthereumBlockSplit("23,24,25");
    assertEquals(ethereumBlockSplit.getBlockIds(), Set.of(BigInteger.valueOf(23), BigInteger.valueOf(24), BigInteger.valueOf(25)));
    assertEquals(ethereumBlockSplit.splitId(), "23,24,25");
  }

  @Test
  void shouldSortBlockIdsInSplitId() {
    EthereumBlockSplit ethereumBlockSplit = new EthereumBlockSplit(
        List.of(BigInteger.valueOf(7), BigInteger.valueOf(3), BigInteger.valueOf(11)));
    assertEquals(ethereumBlockSplit.splitId(), "3,7,11");
  }

  @Test
  void shouldFailWhenTryingToCreateFromAnEmptyListOfBlockIds() {
    assertThrows(IllegalArgumentException.class, () -> new EthereumBlockSplit(List.of()));
  }

  @Test
  void shouldReturnBlockIdsInToString() {
    EthereumBlockSplit ethereumBlockSplit = new EthereumBlockSplit(List.of(BigInteger.valueOf(777), BigInteger.valueOf(778)));
    assertEquals(ethereumBlockSplit.toString(), "EthereumBlockSplit{blockIds=[777, 778]}");
  }

}