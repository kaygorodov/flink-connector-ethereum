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

import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockSplit;
import io.github.kaygorodov.flink.connector.ethereum.split.EthereumBlockRangeSplitState;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import io.github.kaygorodov.flink.connector.ethereum.model.EthBlock;

@Internal
public class EthereumSourceReader extends
    SourceReaderBase<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockSplit, EthereumBlockRangeSplitState> {


    public EthereumSourceReader(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<EthereumBlockWithCheckInfo>> elementsQueue,
        SingleThreadFetcherManager<EthereumBlockWithCheckInfo, EthereumBlockSplit> splitFetcherManager,
        RecordEmitter<EthereumBlockWithCheckInfo, EthBlock, EthereumBlockRangeSplitState> recordEmitter,
        Configuration config, SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, EthereumBlockRangeSplitState> finishedSplitIds) {

    }

    @Override
    protected EthereumBlockRangeSplitState initializedState(EthereumBlockSplit split) {
        return new EthereumBlockRangeSplitState();
    }

    @Override
    protected EthereumBlockSplit toSplitType(String splitId, EthereumBlockRangeSplitState splitState) {
        return new EthereumBlockSplit(splitId);
    }
}
