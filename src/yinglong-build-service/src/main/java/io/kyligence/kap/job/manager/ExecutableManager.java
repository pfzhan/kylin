package io.kyligence.kap.job.manager;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobAddedNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.job.service.JobInfoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.rest.util.SpringContext;

import java.util.List;
import java.util.Map;

@Slf4j
public class ExecutableManager {

    private final KylinConfig config;
    private String project;

    private final NExecutableDao executableDao;
    private final JobInfoService jobInfoService;

    public static ExecutableManager getInstance(KylinConfig config, String project) {
        if (null == project) {
            throw new IllegalStateException();
        }
        return config.getManager(project, ExecutableManager.class);
    }

    private ExecutableManager(KylinConfig config, String project) {
        log.trace("Using metadata url: {}", config);
        this.config = config;
        this.project = project;
        this.executableDao = NExecutableDao.getInstance(config, project);
        this.jobInfoService = SpringContext.getBean(JobInfoService.class);
    }

    public void addJob(ExecutablePO executablePO) {
        addJobOutput(executablePO);
        jobInfoService.addJob(executablePO);

        String jobType = executablePO.getJobType() == null ? "" : executablePO.getJobType().name();
        // dispatch job-created message out
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            EventBusFactory.getInstance().postAsync(new JobReadyNotifier(project));
            EventBusFactory.getInstance().postAsync(new JobAddedNotifier(project, jobType));
        } else
            UnitOfWork.get().doAfterUnit(() -> {
                EventBusFactory.getInstance().postAsync(new JobReadyNotifier(project));
                EventBusFactory.getInstance().postAsync(new JobAddedNotifier(project, jobType));
            });
    }

    private void addJobOutput(ExecutablePO executable) {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executable.setOutput(executableOutputPO);
        if (CollectionUtils.isNotEmpty(executable.getTasks())) {
            for (ExecutablePO subTask : executable.getTasks()) {
                addJobOutput(subTask);
            }
        }
        if (MapUtils.isNotEmpty(executable.getStagesMap())) {
            for (Map.Entry<String, List<ExecutablePO>> entry : executable.getStagesMap().entrySet()) {
                entry.getValue().forEach(this::addJobOutput);
            }
        }
    }
}
