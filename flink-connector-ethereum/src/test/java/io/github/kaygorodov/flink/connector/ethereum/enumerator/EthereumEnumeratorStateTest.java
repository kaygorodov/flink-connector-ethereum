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

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class EthereumEnumeratorStateTest {

  @Test
  void shouldKeepAndReturnLastAssignedBlockNumber() {
      EthereumEnumeratorState ethereumEnumeratorState = new EthereumEnumeratorState(BigInteger.valueOf(33));
      assertEquals(BigInteger.valueOf(33), ethereumEnumeratorState.getLastAssignedBlockNumber());
  }

  @Test
  void shouldNotAcceptNullAsLastAssignedBlockNumber() {
    assertThrows(IllegalArgumentException.class,
        () -> new EthereumEnumeratorState(null));
  }

}