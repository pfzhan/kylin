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
package io.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ACTION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATUS_ILLEGAL;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ExceptionReason;
import org.apache.kylin.common.exception.ExceptionResolve;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.JobExceptionReason;
import org.apache.kylin.common.exception.JobExceptionResolve;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.ChainedExecutable;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.execution.ShellExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import io.kyligence.kap.rest.util.SparkHistoryUIUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

@Component("jobService")
public class JobService extends BasicService implements JobSupporter {

    @Autowired
    private ProjectService projectService;

    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    private static final Logger logger = LoggerFactory.getLogger("schedule");

    private static final Map<String, String> jobTypeMap = Maps.newHashMap();

    private static final String TOTAL_DURATION = "total_duration";
    private static final String LAST_MODIFIED = "last_modified";
    public static final String EXCEPTION_CODE_PATH = "exception_to_code.json";
    public static final String EXCEPTION_CODE_DEFAULT = "KE-030001000";

    static {
        jobTypeMap.put("INDEX_REFRESH", "Refresh Data");
        jobTypeMap.put("INDEX_MERGE", "Merge Data");
        jobTypeMap.put("INDEX_BUILD", "Build Index");
        jobTypeMap.put("INC_BUILD", "Load Data");
        jobTypeMap.put("TABLE_SAMPLING", "Sample Table");
    }

    @Autowired
    public JobService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    @VisibleForTesting
    public ExecutableResponse convert(AbstractExecutable executable) {
        ExecutablePO executablePO = ExecutableManager.toPO(executable, executable.getProject());
        ExecutableResponse executableResponse = ExecutableResponse.create(executable, executablePO);
        executableResponse.setStatus(executable.getStatus().toJobStatus());
        return executableResponse;
    }

    private ExecutableState parseToExecutableState(JobStatusEnum status) {
        Message msg = MsgPicker.getMsg();
        switch (status) {
        case SUICIDAL:
        case DISCARDED:
            return ExecutableState.SUICIDAL;
        case ERROR:
            return ExecutableState.ERROR;
        case FINISHED:
            return ExecutableState.SUCCEED;
        case NEW:
        case READY:
            return ExecutableState.READY;
        case PENDING:
            return ExecutableState.PENDING;
        case RUNNING:
            return ExecutableState.RUNNING;
        case STOPPED:
            return ExecutableState.PAUSED;
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getIllegalExecutableState());
        }
    }

    public void killExistApplication(String project, String jobId) {
        AbstractExecutable job = getManager(ExecutableManager.class, project).getJob(jobId);
        killExistApplication(job);
    }

    public void killExistApplication(AbstractExecutable job) {
        if (job instanceof ChainedExecutable) {
            // if job's task is running spark job, will kill this application
            ((ChainedExecutable) job).getTasks().stream() //
                    .filter(task -> task.getStatus() == ExecutableState.RUNNING) //
                    .filter(task -> task instanceof NSparkExecutable) //
                    .forEach(task -> ((NSparkExecutable) task).killOrphanApplicationIfExists(task.getId()));
        }
    }

    public void setExceptionResolveAndCodeAndReason(Output output, ExecutableStepResponse executableStepResponse) {
        try {
            val exceptionCode = getExceptionCode(output);
            executableStepResponse.setFailedResolve(ExceptionResolve.getResolve(exceptionCode));
            executableStepResponse.setFailedCode(ErrorCode.getLocalizedString(exceptionCode));
            if (StringUtils.equals(exceptionCode, EXCEPTION_CODE_DEFAULT)) {
                val reason = StringUtils.isBlank(output.getFailedReason())
                        ? JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason()
                        : JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": "
                                + output.getFailedReason();
                executableStepResponse.setFailedReason(reason);
            } else {
                executableStepResponse.setFailedReason(ExceptionReason.getReason(exceptionCode));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            executableStepResponse
                    .setFailedResolve(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve());
            executableStepResponse.setFailedCode(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString());
            executableStepResponse
                    .setFailedReason(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason());
        }
    }

    public String getExceptionCode(Output output) {
        try {
            var exceptionOrExceptionMessage = output.getFailedReason();

            if (StringUtils.isBlank(exceptionOrExceptionMessage)) {
                if (StringUtils.isBlank(output.getFailedStack())) {
                    return EXCEPTION_CODE_DEFAULT;
                }
                exceptionOrExceptionMessage = output.getFailedStack().split("\n")[0];
            }

            val exceptionCodeStream = getClass().getClassLoader().getResource(EXCEPTION_CODE_PATH).openStream();
            val exceptionCodes = JsonUtil.readValue(exceptionCodeStream, Map.class);
            for (Object o : exceptionCodes.entrySet()) {
                val exceptionCode = (Map.Entry) o;
                if (StringUtils.contains(exceptionOrExceptionMessage, String.valueOf(exceptionCode.getKey()))
                        || StringUtils.contains(String.valueOf(exceptionCode.getKey()), exceptionOrExceptionMessage)) {
                    val code = exceptionCodes.getOrDefault(exceptionCode.getKey(), EXCEPTION_CODE_DEFAULT);
                    return String.valueOf(code);
                }
            }
            return EXCEPTION_CODE_DEFAULT;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return EXCEPTION_CODE_DEFAULT;
        }
    }

    // for ut
    @VisibleForTesting
    public ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, Output stepOutput,
            Map<String, String> waiteTimeMap, ExecutableState jobState) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(task.getStepId());

        if (stepOutput == null) {
            logger.warn("Cannot found output for task: id={}", task.getId());
            return result;
        }

        result.setStatus(stepOutput.getState().toJobStatus());
        for (Map.Entry<String, String> entry : stepOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        if (KylinConfig.getInstanceFromEnv().isHistoryServerEnable()
                && result.getInfo().containsKey(ExecutableConstants.YARN_APP_ID)) {
            result.putInfo(ExecutableConstants.SPARK_HISTORY_APP_URL,
                    SparkHistoryUIUtil.getHistoryTrackerUrl(result.getInfo().get(ExecutableConstants.YARN_APP_ID)));
        }
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stepOutput));

        result.setDuration(AbstractExecutable.getDuration(stepOutput));
        // if resume job, need sum of waite time
        long waiteTime = Long.parseLong(waiteTimeMap.getOrDefault(task.getId(), "0"));
        if (jobState != ExecutableState.PAUSED) {
            val taskWaitTime = task.getWaitTime();
            // Refactoring: When task Wait Time is equal to waite Time, waiteTimeMap saves the latest waiting time
            if (taskWaitTime != waiteTime) {
                waiteTime = taskWaitTime + waiteTime;
            }
        }
        result.setWaitTime(waiteTime);

        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        result.setShortErrMsg(stepOutput.getShortErrMsg());
        return result;
    }

    @Transaction(project = 0)
    public void updateJobError(String project, String jobId, String failedStepId, String failedSegmentId,
            String failedStack, String failedReason) {
        if (StringUtils.isBlank(failedStepId)) {
            return;
        }
        val executableManager = getManager(NExecutableManager.class, project);
        executableManager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack, failedReason);
    }

    public ExecutableState convertToExecutableState(String status) {
        if (StringUtils.isBlank(status)) {
            return null;
        }
        return ExecutableState.valueOf(status);
    }

    public JobStatisticsResponse getJobStats(String project, long startTime, long endTime) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        Pair<Integer, JobStatistics> stats = manager.getOverallJobStats(startTime, endTime);
        JobStatistics jobStatistics = stats.getSecond();
        return new JobStatisticsResponse(stats.getFirst(), jobStatistics.getTotalDuration(),
                jobStatistics.getTotalByteSize());
    }

    public Map<String, Integer> getJobCount(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        if (dimension.equals("model")) {
            return manager.getJobCountByModel(startTime, endTime);
        }

        return manager.getJobCountByTime(startTime, endTime, dimension);
    }

    public Map<String, Double> getJobDurationPerByte(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        if (dimension.equals("model")) {
            return manager.getDurationPerByteByModel(startTime, endTime);
        }

        return manager.getDurationPerByteByTime(startTime, endTime, dimension);
    }

    public Map<String, Object> getEventsInfoGroupByModel(String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        Map<String, Object> result = Maps.newHashMap();
        result.put("data", null);
        result.put("size", 0);
        return result;
    }

    public void checkJobStatus(List<String> jobStatuses) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        jobStatuses.forEach(this::checkJobStatus);
    }

    public void checkJobStatus(String jobStatus) {
        if (Objects.isNull(JobStatusEnum.getByName(jobStatus))) {
            throw new KylinException(JOB_STATUS_ILLEGAL);
        }
    }

    public void checkJobStatusAndAction(String jobStatus, String action) {
        checkJobStatus(jobStatus);
        JobActionEnum.validateValue(action);
        JobStatusEnum jobStatusEnum = JobStatusEnum.valueOf(jobStatus);
        if (!jobStatusEnum.checkAction(JobActionEnum.valueOf(action))) {
            throw new KylinException(JOB_ACTION_ILLEGAL, jobStatus, jobStatusEnum.getValidActions());
        }

    }

    public void stopBatchJob(String project, TableDesc tableDesc) {
        for (NDataModel tableRelatedModel : getManager(NDataflowManager.class, project)
                .getModelsUsingTable(tableDesc)) {
            stopBatchJobByModel(project, tableRelatedModel.getId());
        }
    }

    private void stopBatchJobByModel(String project, String modelId) {

        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        FusionModel fusionModel = fusionModelManager.getFusionModel(modelId);
        if (!model.isFusionModel() || Objects.isNull(fusionModel)) {
            logger.warn("model is not fusion model or fusion model is null, {}", modelId);
            return;
        }

        ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        executableManager.getJobs().stream().map(executableManager::getJob).filter(
                job -> StringUtils.equalsIgnoreCase(job.getTargetModelId(), fusionModel.getBatchModel().getUuid()))
                .forEach(job -> {
                    Set<ExecutableState> matchedExecutableStates = Stream
                            .of(JobStatusEnum.FINISHED, JobStatusEnum.ERROR, JobStatusEnum.DISCARDED)
                            .map(this::parseToExecutableState).collect(Collectors.toSet());
                    if (!matchedExecutableStates.contains(job.getOutput().getState())) {
                        executableManager.discardJob(job.getId());
                    }
                });
    }

    @Setter
    @Getter
    static class ExecutablePOSortBean {

        private String project;

        private String id;

        @JsonProperty("job_name")
        private String jobName;

        @JsonProperty("last_modified")
        private long lastModified;

        @JsonProperty("target_subject")
        private String targetSubject;

        @JsonProperty("create_time")
        private long createTime;

        @JsonProperty("total_duration")
        private long totalDuration;

        private ExecutablePO executablePO;

        private long computeTotalDuration(ExecutablePO executablePO) {
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (CollectionUtils.isEmpty(tasks)) {
                return 0L;
            }

            long taskCreateTime = executablePO.getOutput().getCreateTime();
            ExecutableState state = ExecutableState.valueOf(executablePO.getOutput().getStatus());
            if (state.isProgressing()) {
                return System.currentTimeMillis() - taskCreateTime;
            }
            long duration = 0L;
            for (ExecutablePO subTask : tasks) {
                if (subTask.getOutput().getStartTime() == 0L) {
                    break;
                }
                duration = getExecutablePOEndTime(subTask) - taskCreateTime;
            }
            return duration == 0L ? getExecutablePOEndTime(executablePO) - taskCreateTime : duration;
        }

        private long getExecutablePOEndTime(ExecutablePO executablePO) {
            long time = executablePO.getOutput().getEndTime();
            return time == 0L ? System.currentTimeMillis() : time;
        }
    }
}
