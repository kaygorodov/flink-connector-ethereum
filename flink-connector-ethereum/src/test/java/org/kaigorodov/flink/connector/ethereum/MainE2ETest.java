package org.kaigorodov.flink.connector.ethereum;

import java.math.BigInteger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class MainE2ETest {
    /*
     * E2E Tests that works against the real eth mainnet API
     *
     * Disabled by default, expect a configured node url via ETH_MAINNET_NODE_URL environment variable
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
        var source = new EthereumBlockSource(mainnetNodeUrl, BigInteger.valueOf(
            20622000
        ));
        var res = env.fromSource(source, WatermarkStrategy.noWatermarks(), "test")
            .executeAndCollect(20);
        System.out.println("******* RESULT ********");
        System.out.println(res);
    }
}