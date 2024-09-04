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