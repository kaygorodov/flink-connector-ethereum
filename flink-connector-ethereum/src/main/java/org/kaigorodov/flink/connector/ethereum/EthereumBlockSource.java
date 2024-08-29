package org.kaigorodov.flink.connector.ethereum;

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
import org.kaigorodov.flink.connector.ethereum.model.EthBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.function.Supplier;

public class EthereumBlockSource implements Source<EthBlock, EthereumBlockRangeSplit, EthereumEnumeratorState> {
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

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<EthereumBlockRangeSplit, EthereumEnumeratorState> createEnumerator(SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext) throws Exception {
        return new EthereumSplitEnumerator(enumContext, initialBlockNumber, ethNodeUrl, null);
    }

    @Override
    public SplitEnumerator<EthereumBlockRangeSplit, EthereumEnumeratorState> restoreEnumerator(
        SplitEnumeratorContext<EthereumBlockRangeSplit> enumContext,
        EthereumEnumeratorState checkpoint) throws Exception {

        return new EthereumSplitEnumerator(enumContext, initialBlockNumber, ethNodeUrl, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<EthereumBlockRangeSplit> getSplitSerializer() {
        return new EthereumBlockRangeSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EthereumEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new EthereumEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<EthBlock, EthereumBlockRangeSplit> createReader(SourceReaderContext readerContext) throws Exception {

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
