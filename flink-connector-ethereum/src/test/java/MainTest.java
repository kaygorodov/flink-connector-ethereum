import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.kaigorodov.flink.connector.ethereum.EthereumBlockSource;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.math.BigInteger;

class MainTest {

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(MainTest.class);
    final public String URL = "";

    @Test
    public void testMain() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var source = new EthereumBlockSource("http://localhost:1234", BigInteger.ONE);
        var res = env.fromSource(source, WatermarkStrategy.noWatermarks(), "test")
//            .map(block -> {
//                System.out.println(block);
//                return block;
//            })
            .executeAndCollect(300);
        System.out.println("******* RESULT ********");
        System.out.println(res);
    }
}