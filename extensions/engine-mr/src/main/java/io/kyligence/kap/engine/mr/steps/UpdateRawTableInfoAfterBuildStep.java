package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

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

public class UpdateRawTableInfoAfterBuildStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateRawTableInfoAfterBuildStep.class);

    public UpdateRawTableInfoAfterBuildStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final RawTableManager rawManager = RawTableManager.getInstance(context.getConfig());
        final RawTableInstance rawInstance = rawManager.getRawTableInstance(CubingExecutableUtil.getCubeName(this.getParams()));
        final RawTableSegment segment = rawInstance.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        CubingJob cubingJob = (CubingJob) executableManager.getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        long sourceCount = cubingJob.findSourceRecordCount();
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();

        segment.setLastBuildJobID(CubingExecutableUtil.getCubingJobId(this.getParams()));
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setSizeKB(cubeSizeBytes / 1024);
        segment.setInputRecords(sourceCount);
        segment.setInputRecordsSize(sourceSizeBytes);

        try {
            rawManager.promoteNewlyBuiltSegments(rawInstance, segment);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update rawTable after build", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}
