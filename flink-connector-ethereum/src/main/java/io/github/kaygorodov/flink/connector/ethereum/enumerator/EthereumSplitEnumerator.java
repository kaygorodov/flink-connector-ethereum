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

import io.github.kaygorodov.flink.connector.ethereum.client.EthNetworkClient;
import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockRangeSplit;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

@Internal
public class EthereumSplitEnumerator implements SplitEnumerator<EthereumBlockRangeSplit, EthereumEnumeratorState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumSplitEnumerator.class);

    private final String ethNodeUrl;
    private final BigInteger initialBlockNumber;
    private BigInteger lastAssignedBlockNumber;
    private final SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext;
    private final BigInteger DEFAULT_BATCH_SIZE = BigInteger.valueOf(5);
    private final EthNetworkClient ethNetworkClient;
    private BigInteger latestBlockNumber;

    public EthereumSplitEnumerator(SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext,
                                    BigInteger initialBlockNumber, String ethNodeUrl, EthereumEnumeratorState checkpoint) {
        this.enumContext = enumContext;
        this.initialBlockNumber = initialBlockNumber;
        if (checkpoint != null) {
            logger.info("Recovering EthereumSplitEnumerator from state {}", checkpoint);
            this.lastAssignedBlockNumber = checkpoint.getLastAssignedBlockNumber();
        }
        this.ethNodeUrl = ethNodeUrl;
        this.ethNetworkClient = new EthNetworkClient(ethNodeUrl);
        latestBlockNumber = this.ethNetworkClient.getLatestBlockNumber();
    }

    @Override
    public void start() {
        logger.info("Starting EthereumSourceEnumerator using Node URL: {}", ethNodeUrl);
        logger.info("Initial Block Number is {}", initialBlockNumber);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        logger.info("Subtask {} requested a new split", subtaskId);

        if (lastAssignedBlockNumber == null) {
            lastAssignedBlockNumber = initialBlockNumber.subtract(BigInteger.ONE);
        }

        BigInteger batchSize = DEFAULT_BATCH_SIZE.min(latestBlockNumber.subtract(lastAssignedBlockNumber));

        if (batchSize.compareTo(BigInteger.ZERO) <= 0) {
            logger.info("Reached the latest block. Do nothing for now");
            return;
        }

        logger.info("Assigning batch size {}", batchSize);

        this.enumContext.assignSplit(new EthereumBlockRangeSplit(
            Stream.iterate(lastAssignedBlockNumber.add(BigInteger.ONE), n -> n.add(BigInteger.ONE)).limit(
                batchSize.longValue()).toList()
        ), subtaskId);

        lastAssignedBlockNumber = lastAssignedBlockNumber.add(batchSize);
    }

    @Override
    public void addSplitsBack(List<EthereumBlockRangeSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {
        logger.info("add reader request for subtaskid {}", subtaskId);
        this.handleSplitRequest(subtaskId, null);

    }

    @Override
    public EthereumEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new EthereumEnumeratorState(lastAssignedBlockNumber);
    }

    @Override
    public void close() throws IOException {

    }
}
