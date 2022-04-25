package io.kyligence.kap.job.manager;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.AddIndexHandler;
import org.apache.kylin.job.handler.AddSegmentHandler;
import org.apache.kylin.job.handler.MergeSegmentHandler;
import org.apache.kylin.job.handler.RefreshSegmentHandler;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.model.JobParam;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STORAGE_QUOTA_LIMIT;

@Slf4j
public class JobManager {

    private KylinConfig config;

    private String project;

    public static org.apache.kylin.job.manager.JobManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, org.apache.kylin.job.manager.JobManager.class);
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
