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

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.cube.raw.RawTableUpdate;

public class RawShardSizingStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(RawShardSizingStep.class);

    public RawShardSizingStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig kylinConf = context.getConfig();
        String rawName = CubingExecutableUtil.getCubeName(this.getParams());
        String segmentId = CubingExecutableUtil.getSegmentId(this.getParams());
        RawTableInstance rawInstance = RawTableManager.getInstance(kylinConf).getRawTableInstance(rawName);
        RawTableSegment seg = rawInstance.getSegmentById(segmentId);

        try {
            rawShardSizing(rawInstance, seg, kylinConf);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to save rawtable statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private void rawShardSizing(RawTableInstance rawInstance, RawTableSegment seg, KylinConfig kylinConf) throws IOException {
        KapConfig kapConfig = KapConfig.wrap(kylinConf);
        int mbPerShard = kapConfig.getParquetStorageShardSize();
        int shardMax = kapConfig.getParquetStorageShardMax();
        int shardMin = kapConfig.getParquetStorageShardMin();

        double estimatedSize = caculateEstimateStorageSize(seg);
        logger.info("raw table estimated size {} MB", estimatedSize);
        int shardNum = (int) (estimatedSize / mbPerShard);

        if (shardNum > shardMax) {
            logger.info(String.format("RawTable's estimated size %.2f MB will generate %d regions, reduce to %d", estimatedSize, shardNum, shardMax));
            shardNum = shardMax;
        } else if (shardNum < shardMin) {
            logger.info(String.format("RawTable's estimated size %.2f MB will generate %d regions, increase to %d", estimatedSize, shardNum, shardMin));
            shardNum = shardMin;
        } else {
            logger.info(String.format("RawTable's estimated size %.2f MB will generate %d regions", estimatedSize, shardNum));
        }
        if (null != rawInstance) {
            seg.setShardNum(shardNum);
            rawInstance.setShardNumber(rawInstance.getShardNumber() + shardNum);
            RawTableUpdate builder = new RawTableUpdate(rawInstance);
            builder.setToUpdateSegs(seg);
            RawTableManager.getInstance(kylinConf).updateRawTable(builder);
        }
    }

    private double caculateEstimateStorageSize(RawTableSegment seg) throws IOException {
        CubingJob cubingJob = (CubingJob) getManager().getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        return (1.0 * sourceSizeBytes) / (1024L * 1024L);

        //        JobEngineConfig conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        //        String jobID = CubingExecutableUtil.getCubingJobId(getParams());
        //        final String rowCountOutputDir = JobBuilderSupport.getJobWorkingDir(conf, jobID) + "/row_count";
        //        Path rowCountFile = new Path(rowCountOutputDir, "000000_0");
        //
        //        Long nRow = 0L;
        //        FileSystem fs = FileSystem.get(rowCountFile.toUri(), HadoopUtil.getCurrentConfiguration());
        //        InputStream in = fs.open(rowCountFile);
        //        try {
        //            String content = IOUtils.toString(in, Charset.defaultCharset());
        //            nRow = Long.valueOf(content.trim()); // strip the '\n' character
        //
        //        } finally {
        //            IOUtils.closeQuietly(in);
        //        }
        //        double ret = 1.0 * seg.getRawTableInstance().getRawTableDesc().getEstimateRowSize() * nRow / (1024L * 1024L);
        //        return ret;
    }
}
