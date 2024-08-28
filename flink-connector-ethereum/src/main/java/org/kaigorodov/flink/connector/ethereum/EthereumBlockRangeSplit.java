package org.kaigorodov.flink.connector.ethereum;

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

    public EthereumBlockRangeSplit(BigInteger blockId) {
        this(List.of(blockId));
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
