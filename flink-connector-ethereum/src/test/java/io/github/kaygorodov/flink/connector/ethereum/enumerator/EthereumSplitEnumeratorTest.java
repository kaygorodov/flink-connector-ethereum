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
package io.github.kaygorodov.flink.connector.ethereum.enumerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.kaygorodov.flink.connector.ethereum.client.EthNetworkClient;
import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockSplit;
import java.math.BigInteger;
import java.util.List;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.junit.jupiter.api.Test;

class EthereumSplitEnumeratorTest {

  @Test
  void shouldUseInitialBlockNumberWhenStartWithoutState() {
    var context = mock(SplitEnumeratorContext.class);
    var client = mock(EthNetworkClient.class);
    final var initialBlockNumber = BigInteger.valueOf(33);
    EthereumEnumeratorState state = null; // i.e. no checkpoint from which it recovers

    when(client.getLatestBlockNumber()).thenReturn(BigInteger.valueOf(100));
    EthereumSplitEnumerator ethereumSplitEnumerator = new EthereumSplitEnumerator(
        context, initialBlockNumber, client, state);
    ethereumSplitEnumerator.start();
    ethereumSplitEnumerator.handleSplitRequest(12, "requesterHostName");

    verify(context).assignSplit(
        new EthereumBlockSplit(List.of(
            initialBlockNumber,
            initialBlockNumber.add(BigInteger.ONE),
            initialBlockNumber.add(BigInteger.TWO),
            initialBlockNumber.add(BigInteger.valueOf(3)),
            initialBlockNumber.add(BigInteger.valueOf(4))
        )),
        12
    );

  }

}