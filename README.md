# Flink Ethereum Connector

Flink DataStream connector for any Ethereum-compatible network

# Pre Req

The current version of the connector is built using Flink 1.20 and JDK 17

# Usage

```java
var startFromBlock = BigInteger.valueOf(20622000);
var source = new EthereumBlockSource(
    "https://eth_network_node_url",
    startFromBlock,
    RateLimiterStrategy.perSecond(0.5)
);
env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "My Eth Source")
    .print();

env.execute();
```

# Contribute

Feel free to raise a GitHub issue, or to fork and open a pull request.

Do you have an idea or suggestion on how to improve the project? Please write to andreykaygorodov@gmail.com

# License

Flink Ethereum Connector is released under version 2.0 of the Apache License.
