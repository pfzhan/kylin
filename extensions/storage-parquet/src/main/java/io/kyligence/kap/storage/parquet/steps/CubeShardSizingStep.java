/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
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
public class CubeShardSizingStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CubeShardSizingStep.class);
    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    public CubeShardSizingStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        CubeSegment newSegment = CubingExecutableUtil.findSegment(context, CubingExecutableUtil.getCubeName(this.getParams()), CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = newSegment.getConfig();

        try {
            cuboidShardSizing(newSegment, kylinConf);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());
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
        int shardMin = kapConfig.getParquetStorageShardMin();

        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidSizeMap.keySet());
        Collections.sort(allCuboids);

        if (!seg.isEnableSharding()) {
            throw new IllegalStateException("Shard must be enabled");
        }

        HashMap<Long, Short> cuboidShards = Maps.newHashMap();
        for (long cuboidId : allCuboids) {
            double estimatedSize = cuboidSizeMap.get(cuboidId);
            int shardNum = (int) Math.ceil(1.0 * estimatedSize / mbPerShard);

            if (shardNum > shardMax) {
                shardNum = shardMax;
            } else if (shardNum < shardMin) {
                shardNum = shardMin;
            }

            cuboidShards.put(cuboidId, (short) shardNum);
        }

        CuboidShardUtil.saveCuboidShards(seg, cuboidShards, 0);

    }

}
