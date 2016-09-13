package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.util.List;

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

public class UpdateRawTableInfoAfterMergeStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(UpdateRawTableInfoAfterMergeStep.class);

    private final RawTableManager rawManager = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());

    public UpdateRawTableInfoAfterMergeStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final RawTableInstance raw = rawManager.getRawTableInstance(CubingExecutableUtil.getCubeName(this.getParams()));

        RawTableSegment mergedSegment = raw.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        if (mergedSegment == null) {
            return new ExecuteResult(ExecuteResult.State.FAILED, "there is no segment with id:" + CubingExecutableUtil.getSegmentId(this.getParams()));
        }

        CubingJob cubingJob = (CubingJob) executableManager.getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();

        // collect source statistics
        List<String> mergingSegmentIds = CubingExecutableUtil.getMergingSegmentIds(this.getParams());
        if (mergingSegmentIds.isEmpty()) {
            return new ExecuteResult(ExecuteResult.State.FAILED, "there are no merging segments");
        }
        long sourceCount = 0L;
        long sourceSize = 0L;
        for (String id : mergingSegmentIds) {
            RawTableSegment segment = raw.getSegmentById(id);
            sourceCount += segment.getInputRecords();
            sourceSize += segment.getInputRecordsSize();
        }

        // update segment info
        mergedSegment.setSizeKB(cubeSizeBytes / 1024);
        mergedSegment.setInputRecords(sourceCount);
        mergedSegment.setInputRecordsSize(sourceSize);
        mergedSegment.setLastBuildJobID(CubingExecutableUtil.getCubingJobId(this.getParams()));
        mergedSegment.setIndexPath(CubingExecutableUtil.getIndexPath(this.getParams()));
        mergedSegment.setLastBuildTime(System.currentTimeMillis());

        try {
            rawManager.promoteNewlyBuiltSegments(raw, mergedSegment);
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        } catch (IOException e) {
            logger.error("fail to update rawtable after merge", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}
