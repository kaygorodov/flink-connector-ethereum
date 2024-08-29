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
package org.kaigorodov.flink.connector.ethereum;

import java.util.stream.Stream;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class EthereumSplitEnumerator implements SplitEnumerator<EthereumBlockRangeSplit, EthereumEnumeratorState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumSplitEnumerator.class);

    private String ethNodeUrl;
    private BigInteger initialBlockNumber;
    private BigInteger lastAssignedBlockNumber;
    private SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext;


    public EthereumSplitEnumerator(SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext,
                                    BigInteger initialBlockNumber, String ethNodeUrl, EthereumEnumeratorState checkpoint) {
        this.enumContext = enumContext;
        this.initialBlockNumber = initialBlockNumber;
        if (checkpoint != null) {
            logger.info("Recovering EthereumSplitEnumerator from state {}", checkpoint);
            this.lastAssignedBlockNumber = checkpoint.getLastAssignedBlockNumber();
        }
        this.ethNodeUrl = ethNodeUrl;
    }

    @Override
    public void start() {
        logger.info("Starting EthereumSourceEnumerator using Node URL: {}", ethNodeUrl);
        logger.info("Initial Block Number is {}", initialBlockNumber);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        logger.info("Subtask {} requested a new split", subtaskId);

        if(lastAssignedBlockNumber != null &&
            initialBlockNumber.add(BigInteger.valueOf(40)).compareTo(lastAssignedBlockNumber) < 0) {
            return;
        }

        BigInteger batchSize = BigInteger.valueOf(5);

        if (lastAssignedBlockNumber == null) {
            lastAssignedBlockNumber = initialBlockNumber.subtract(BigInteger.ONE);
        }

        this.enumContext.assignSplit(new EthereumBlockRangeSplit(
            Stream.iterate(lastAssignedBlockNumber.add(BigInteger.ONE), n -> n.add(BigInteger.ONE)).limit(batchSize.longValue()).toList()
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
