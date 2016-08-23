package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableSegment;

public class RawShardSizingStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(RawShardSizingStep.class);

    private String jobId;

    public RawShardSizingStep(String jobId) {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        CubeSegment newSegment = CubingExecutableUtil.findSegment(context, CubingExecutableUtil.getCubeName(this.getParams()), CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = newSegment.getConfig();

        try {
            rawShardSizing(newSegment, kylinConf);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to save rawtable statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private void rawShardSizing(CubeSegment seg, KylinConfig kylinConf) throws IOException {
        KapConfig kapConfig = KapConfig.wrap(kylinConf);
        int mbPerShard = kapConfig.getParquetStorageShardSize();
        int shardMax = kapConfig.getParquetStorageShardMax();
        int shardMin = kapConfig.getParquetStorageShardMin();
        RawTableSegment rawSeg = RawTableSegment.getInstance(seg);

        if (!seg.isEnableSharding()) {
            throw new IllegalStateException("Shard must be enabled");
        }

        double estimatedSize = caculateEstimateStorageSize(rawSeg);
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

    }

    private double caculateEstimateStorageSize(RawTableSegment seg) throws IOException {
        JobEngineConfig conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        final String rowCountOutputDir = JobBuilderSupport.getJobWorkingDir(conf, jobId) + "/row_count";
        Path rowCountFile = new Path(rowCountOutputDir, "000000_0");

        Long nRow;
        FileSystem fs = FileSystem.get(rowCountFile.toUri(), HadoopUtil.getCurrentConfiguration());
        InputStream in = fs.open(rowCountFile);
        try {
            String content = IOUtils.toString(in, Charset.defaultCharset());
            nRow = Long.valueOf(content.trim()); // strip the '\n' character

        } finally {
            IOUtils.closeQuietly(in);
        }
        double ret = 1.0 * seg.getRawTableInstance().getRawTableDesc().getEstimateRowSize() * nRow / (1024L * 1024L);
        return ret;
    }
}
