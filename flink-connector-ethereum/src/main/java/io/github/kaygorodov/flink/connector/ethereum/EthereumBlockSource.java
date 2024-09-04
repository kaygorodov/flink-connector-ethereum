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

import io.github.kaygorodov.flink.connector.ethereum.enumerator.EthereumEnumeratorState;
import io.github.kaygorodov.flink.connector.ethereum.enumerator.EthereumEnumeratorStateSerializer;
import io.github.kaygorodov.flink.connector.ethereum.enumerator.EthereumSplitEnumerator;
import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockSplit;
import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockRangeSplitReader;
import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockRangeSplitSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import io.github.kaygorodov.flink.connector.ethereum.model.EthBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.function.Supplier;

public class EthereumBlockSource implements Source<EthBlock, EthereumBlockSplit, EthereumEnumeratorState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumBlockSource.class);
    private final String ethNodeUrl;
    private final BigInteger initialBlockNumber;
    private final RateLimiterStrategy rateLimiterStrategy;

    public EthereumBlockSource(String ethNodeUrl, BigInteger initialBlockNumber, RateLimiterStrategy rateLimiterStrategy) {
        logger.info("Init source with ethNodeUrl {} and initial block number {}", ethNodeUrl, initialBlockNumber);
        this.ethNodeUrl = ethNodeUrl;
        this.initialBlockNumber = initialBlockNumber;
        this.rateLimiterStrategy = rateLimiterStrategy;
    }

    public static EthereumBlockSourceBuilder builder() {
        return new EthereumBlockSourceBuilder();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<EthereumBlockSplit, EthereumEnumeratorState> createEnumerator(SplitEnumeratorContext<EthereumBlockSplit> enumContext) throws Exception {
        return new EthereumSplitEnumerator(enumContext, initialBlockNumber, ethNodeUrl, null);
    }

    @Override
    public SplitEnumerator<EthereumBlockSplit, EthereumEnumeratorState> restoreEnumerator(
        SplitEnumeratorContext<EthereumBlockSplit> enumContext,
        EthereumEnumeratorState checkpoint) throws Exception {

        return new EthereumSplitEnumerator(enumContext, initialBlockNumber, ethNodeUrl, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<EthereumBlockSplit> getSplitSerializer() {
        return new EthereumBlockRangeSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EthereumEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new EthereumEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<EthBlock, EthereumBlockSplit> createReader(SourceReaderContext readerContext) throws Exception {

        logger.info("Creating a reader from source impl");
        FutureCompletingBlockingQueue<RecordsWithSplitIds<EthereumBlockWithCheckInfo>>
            elementsQueue = new FutureCompletingBlockingQueue<>();

        Supplier<EthereumBlockRangeSplitReader> splitReaderSupplier = () -> new EthereumBlockRangeSplitReader(readerContext, ethNodeUrl);

        EthereumSplitFetcherManager ethereumSplitFetcherManager = new EthereumSplitFetcherManager(
            elementsQueue,
            splitReaderSupplier::get
        );

        EthereumBlockEmitter emitter = new EthereumBlockEmitter();

        int parallelism = readerContext.currentParallelism();
        RateLimiter rateLimiter = rateLimiterStrategy.createRateLimiter(parallelism);
        return new RateLimitedSourceReader<>(
            new EthereumSourceReader(elementsQueue, ethereumSplitFetcherManager, emitter, new Configuration(), readerContext),
            rateLimiter);
    }
}
