package io.kyligence.kap.job.manager;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import io.kyligence.kap.job.handler.AbstractJobHandler;
import io.kyligence.kap.job.handler.AddIndexHandler;
import io.kyligence.kap.job.handler.AddSegmentHandler;
import io.kyligence.kap.job.handler.MergeSegmentHandler;
import io.kyligence.kap.job.handler.RefreshSegmentHandler;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.model.JobParam;

import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STORAGE_QUOTA_LIMIT;

@Slf4j
public class JobManager {

    private KylinConfig config;

    private String project;

    public JobManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
    }

    // called by reflection
    @SuppressWarnings("unused")
    static JobManager newInstance(KylinConfig conf, String project) {
        checkNotNull(project);
        return new JobManager(conf, project);
    }

    public static JobManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, JobManager.class);
    }

    public String addJob(JobParam jobParam, AbstractJobHandler handler) {
        if (!config.isJobNode() && !config.isUTEnv()) {
            throw new KylinException(JOB_CREATE_ABANDON);
        }
        checkNotNull(project);
        checkStorageQuota(project);
        jobParam.setProject(project);
        ExecutableUtil.computeParams(jobParam);

        AbstractJobHandler localHandler = handler != null ? handler : createJobHandler(jobParam);
        if (localHandler == null)
            return null;

        localHandler.handle(jobParam);
        return jobParam.getJobId();
    }

    public String addJob(JobParam jobParam) {
        return addJob(jobParam, null);
    }

    public String refreshSegmentJob(JobParam jobParam) {
        return refreshSegmentJob(jobParam, false);
    }

    public String refreshSegmentJob(JobParam jobParam, boolean refreshAllLayouts) {
        jobParam.getCondition().put(JobParam.ConditionConstant.REFRESH_ALL_LAYOUTS, refreshAllLayouts);
        NDataModel model = NDataModelManager.getInstance(config, project).getDataModelDesc(jobParam.getModel());
        if (model.isMultiPartitionModel() && CollectionUtils.isNotEmpty(jobParam.getTargetPartitions())) {
            jobParam.setJobTypeEnum(JobTypeEnum.SUB_PARTITION_REFRESH);
        } else {
            jobParam.setJobTypeEnum(JobTypeEnum.INDEX_REFRESH);
        }
        return addJob(jobParam);
    }

    public String mergeSegmentJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.INDEX_MERGE);
        return addJob(jobParam);
    }

    public String addRelatedIndexJob(JobParam jobParam) {
        boolean noNeed = (jobParam.getTargetSegments() == null
                && SegmentUtil.getValidSegments(jobParam.getModel(), project).isEmpty())
                || (jobParam.getTargetSegments() != null && jobParam.getTargetSegments().isEmpty());
        if (noNeed) {
            log.debug("No need to add index build job due to there is no valid segment in {}.", jobParam.getModel());
            return null;
        }
        jobParam.setJobTypeEnum(JobTypeEnum.INDEX_BUILD);
        return addJob(jobParam);
    }

    public String addIndexJob(JobParam jobParam) {
        val relatedSegments = SegmentUtil.getValidSegments(jobParam.getModel(), project).stream()
                .map(NDataSegment::getId).collect(Collectors.toSet());
        jobParam.withTargetSegments(relatedSegments);
        return addRelatedIndexJob(jobParam);
    }

    public String addSegmentJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.INC_BUILD);
        return addJob(jobParam);
    }

    public String buildPartitionJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.SUB_PARTITION_BUILD);
        return addJob(jobParam);
    }

    private AbstractJobHandler createJobHandler(JobParam jobParam) {
        AbstractJobHandler handler;
        switch (jobParam.getJobTypeEnum()) {
            case INC_BUILD:
                handler = new AddSegmentHandler();
                break;
            case INDEX_MERGE:
                handler = new MergeSegmentHandler();
                break;
            case INDEX_BUILD:
            case SUB_PARTITION_BUILD:
                handler = new AddIndexHandler();
                break;
            case INDEX_REFRESH:
            case SUB_PARTITION_REFRESH:
                handler = new RefreshSegmentHandler();
                break;
            case EXPORT_TO_SECOND_STORAGE:
                throw new UnsupportedOperationException();
            default:
                log.error("jobParam doesn't have matched job: {}", jobParam.getJobTypeEnum());
                return null;
        }
        return handler;
    }

    public static void checkStorageQuota(String project) {
        val scheduler = NDefaultScheduler.getInstance(project);
        if (scheduler.hasStarted() && scheduler.getContext().isReachQuotaLimit()) {
            log.error("Add job failed due to no available storage quota in project {}", project);
            throw new KylinException(JOB_STORAGE_QUOTA_LIMIT);
        }
    }
}
