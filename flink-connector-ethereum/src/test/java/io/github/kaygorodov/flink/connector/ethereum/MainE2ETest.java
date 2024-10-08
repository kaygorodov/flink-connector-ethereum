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

import io.github.kaygorodov.flink.connector.ethereum.EthereumBlockSource;
import java.math.BigInteger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class MainE2ETest {
    /*
     * E2E Tests that works against the real eth mainnet API
     *
     * Disabled by default, expects a configured node url via ETH_MAINNET_NODE_URL environment variable
     */
    @Disabled
    @Test
    void testMain() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var mainnetNodeUrl = System.getenv("ETH_MAINNET_NODE_URL");
        if(mainnetNodeUrl == null) {
            throw new RuntimeException("ETH_MAINNET_NODE_URL env variable is not defined");
        }

        var startFromBlock = BigInteger.valueOf(20640708);

        var source = EthereumBlockSource.builder()
            .setEthNodeUrl(mainnetNodeUrl)
            .setInitialBlockNumber(startFromBlock)
            .setRateLimiterStrategy(RateLimiterStrategy.perSecond(1))
            .build();

        env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "test")
            .print();

        env.execute();
    }
}