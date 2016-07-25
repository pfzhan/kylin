/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CuboidShardUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Save the cube segment statistic to Kylin metadata store
 */
public class ParquetShardSizingStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(ParquetShardSizingStep.class);

    public ParquetShardSizingStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        CubeSegment newSegment = CubingExecutableUtil.findSegment(context, CubingExecutableUtil.getCubeName(this.getParams()), CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = newSegment.getConfig();

        try {
            cuboidShardSizing(newSegment, kylinConf);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to save cuboid statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private void cuboidShardSizing(CubeSegment seg, KylinConfig kylinConf) throws IOException {
        KapConfig kapConfig = KapConfig.wrap(kylinConf);
        Map<Long, Double> cuboidSizeMap = new CubeStatsReader(seg, kylinConf).getCuboidSizeMap();
        int mbPerShard = kapConfig.getParquetStorageShardSize();
        int shardMax = kapConfig.getParquetStorageShardMax();
        int shardMin = kapConfig.getParquetPageIndexStepMin();

        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidSizeMap.keySet());
        Collections.sort(allCuboids);

        if (!seg.isEnableSharding()) {
            throw new IllegalStateException("Shard must be enabled");
        }

        HashMap<Long, Short> cuboidShards = Maps.newHashMap();
        for (long cuboidId : allCuboids) {
            double estimatedSize = cuboidSizeMap.get(cuboidId);
            int shardNum = (int) (estimatedSize / mbPerShard);

            if (shardNum > shardMax) {
                logger.info(String.format("Cuboid %d 's estimated size %.2f MB will generate %d regions, reduce to %d", cuboidId, estimatedSize, shardNum, shardMax));
                shardNum = shardMax;
            } else if (shardNum < shardMin) {
                logger.info(String.format("Cuboid %d 's estimated size %.2f MB will generate %d regions, increase to %d", cuboidId, estimatedSize, shardNum, shardMin));
                shardNum = shardMin;
            } else {
                logger.info(String.format("Cuboid %d 's estimated size %.2f MB will generate %d regions", cuboidId, estimatedSize, shardNum));
            }

            cuboidShards.put(cuboidId, (short) shardNum);
        }

        CuboidShardUtil.saveCuboidShards(seg, cuboidShards, 0);

    }

}
