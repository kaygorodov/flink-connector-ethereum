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
package io.github.kaygorodov.flink.connector.ethereum;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import io.github.kaygorodov.flink.connector.ethereum.model.EthBlock;

class EthereumBlockRangeSplitReaderTest {

  @Test
  void shouldReturnEmptyRecordResultOnFetchWhenNoSplitsAssigned() {
    var reader = new EthereumBlockRangeSplitReader(
        mock(SourceReaderContext.class), mock(EthNetworkClient.class)
    );

    var records = reader.fetch();
    Assertions.assertNull(records.nextRecordFromSplit());
    Assertions.assertNull(records.nextSplit());
  }

  @Test
  void shouldReturnAllRecordsFromAssignedSplitAndThenFinishIt() {
    var ethClient = mock(EthNetworkClient.class);
    var reader = new EthereumBlockRangeSplitReader(
        mock(SourceReaderContext.class), ethClient
    );

    var blockNumberOne = BigInteger.valueOf(777);
    var blockNumberTwo = BigInteger.valueOf(778);
    reader.handleSplitsChanges(new SplitsAddition<>(List.of(
        new EthereumBlockRangeSplit(List.of(blockNumberOne, blockNumberTwo))
    )));

    when(ethClient.getBlockByNumber(blockNumberOne)).thenReturn(createBlock(blockNumberOne));
    when(ethClient.getBlockByNumber(blockNumberTwo)).thenReturn(createBlock(blockNumberTwo));

    var records = reader.fetch();
    Assertions.assertEquals(records.nextRecordFromSplit().getEthBlock().getNumber(), blockNumberOne);
    Assertions.assertEquals(records.nextRecordFromSplit().getEthBlock().getNumber(), blockNumberTwo);
    Assertions.assertEquals(records.finishedSplits(), Set.of("777,778"));
  }

  private EthBlock createBlock(BigInteger number) {
    var block = new EthBlock();
    block.setNumber(number);
    block.setHash(number.toString());
    return block;
  }

}