import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.kaigorodov.flink.connector.ethereum.EthereumBlockSource;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.math.BigInteger;

class MainTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MainTest.class);
    final String URL = "";

    @Test
    public void testMain() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var source = new EthereumBlockSource("http://localhost:1234", new BigInteger("123123123"));
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test")
                .print();
        env.execute();
    }
}