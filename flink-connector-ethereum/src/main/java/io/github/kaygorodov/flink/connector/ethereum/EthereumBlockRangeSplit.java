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

import org.apache.flink.api.connector.source.SourceSplit;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Defines a number of blocks that belong to a given Split.
 */
public class EthereumBlockRangeSplit implements SourceSplit {

    public List<BigInteger> getBlockIds() {
        return blockIds;
    }

    private final List<BigInteger> blockIds;

    public EthereumBlockRangeSplit(List<BigInteger> blockIds) {
        this.blockIds = blockIds;
    }

    public EthereumBlockRangeSplit(String splitId) {
        this(Arrays.stream(splitId.split(",")).map(BigInteger::new).collect(Collectors.toList()));
    }

    @Override
    public String splitId() {
        return blockIds.stream().map(BigInteger::toString).collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return "EthereumBlockRangeSplit{" +
            "blockIds=" + blockIds +
            '}';
    }
}