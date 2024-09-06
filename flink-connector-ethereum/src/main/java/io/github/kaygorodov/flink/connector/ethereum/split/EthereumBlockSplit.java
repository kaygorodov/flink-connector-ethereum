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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * A split of Ethereum blocks
 */
@Internal
public class EthereumBlockSplit implements SourceSplit {

    /**
     * Returns the block ids that constitute this split
     */
    public Set<BigInteger> getBlockIds() {
        return blockIds;
    }

    private final Set<BigInteger> blockIds;

    public EthereumBlockSplit(List<BigInteger> blockIds) {
        if (blockIds.isEmpty()) {
            throw new IllegalArgumentException("blockIds must not be empty");
        }
        this.blockIds = new HashSet<>(blockIds);
    }

    /**
     * Creates a split from a comma-separated list of block ids
     */
    public EthereumBlockSplit(String splitId) {
        this(Arrays.stream(splitId.split(",")).map(BigInteger::new).toList());
    }

    @Override
    public String splitId() {
        return String.join(",", blockIds.stream().sorted().map(BigInteger::toString).toList());
    }

    @Override
    public String toString() {
        return "EthereumBlockSplit{blockIds=" + blockIds + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EthereumBlockSplit that)) {
            return false;
        }
      return Objects.equals(blockIds, that.blockIds);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(blockIds);
    }
}
