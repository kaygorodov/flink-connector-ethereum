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

import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockRangeSplitState;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import io.github.kaygorodov.flink.connector.ethereum.model.EthBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Internal
public class EthereumBlockEmitter implements RecordEmitter<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockRangeSplitState> {
    private static final Logger logger = LoggerFactory.getLogger(EthereumBlockEmitter.class);

    @Override
    public void emitRecord(EthereumBlockWithCheckInfo element, SourceOutput<EthBlock> output, EthereumBlockRangeSplitState splitState) throws Exception {
        logger.info("Emitting record {}", element.getEthBlock());
        output.collect(
            element.getEthBlock()
        );

    }
}
