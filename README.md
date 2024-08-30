# Flink Ethereum Connector

Flink DataStream connector for any Ethereum-compatible network

# Prerequisites

The current version of the connector is built using Flink 1.20 and JDK 17

# Usage

Add the following dependency to your project:

```xml
<dependency>
    <groupId>io.github.kaygorodov</groupId>
    <artifactId>flink-connector-ethereum</artifactId>
    <version>0.1.0</version>
</dependency>
```

The source could be created as follows:

```java
var startFromBlock = BigInteger.valueOf(20622000);
var source = EthereumBlockSource.builder()
    .setEthNodeUrl(mainnetNodeUrl)
    .setInitialBlockNumber(startFromBlock)
    .setRateLimiterStrategy(RateLimiterStrategy.perSecond(1))
    .build();
env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "My Eth Source")
    .print();

env.execute();
```

# Contribute

Feel free to raise a GitHub issue, or to fork and open a pull request.

Do you have an idea or suggestion on how to improve the project? Please write to andreykaygorodov@gmail.com

# Releasing

Release versions are published to Maven Central

https://central.sonatype.com/artifact/io.github.kaygorodov/flink-connector-ethereum

# License

Flink Ethereum Connector is released under version 2.0 of the Apache License.
