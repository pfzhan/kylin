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

package org.apache.kylin.job.execution;

import static org.apache.kylin.job.constant.ExecutableConstants.MR_JOB_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_URL;

import java.io.IOException;
import java.util.Collections;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobSuicideException;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

/**
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    protected static final String SUBMITTER = "submitter";
    protected static final String NOTIFY_LIST = "notify_list";
    protected static final String PARENT_ID = "parentId";
    public static final String RUNTIME_INFO = "runtimeInfo";
    public static final String DEPENDENT_FILES = "dependentFiles";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);
    protected int retry = 0;

    private KylinConfig config;
    @Getter
    @Setter
    private String name;
    @Getter
    @Setter
    private JobTypeEnum jobType;

    @Setter
    @Getter
    private String targetModel;// uuid of the model

    @Setter
    @Getter
    private List<String> targetSegments = Lists.newArrayList();//uuid of related segments

    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private long dataRangeStart;

    @Getter
    @Setter
    private long dataRangeEnd;

    private Map<String, String> params = Maps.newHashMap();
    private String project;

    @Getter
    @Setter
    private Map<String, Object> runTimeInfo = Maps.newHashMap();

    public String getTargetModelAlias() {
        NDataModel dataModelDesc = NDataModelManager.getInstance(config, getProject()).getDataModelDesc(targetModel);
        return (dataModelDesc == null || dataModelDesc.isBroken()) ? null : dataModelDesc.getAlias();
    }

    private boolean checkTargetSegmentExists(String segmentId) {
        NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(targetModel);
        if (dataflow == null) {
            return false;
        }
        NDataSegment segment = dataflow.getSegment(segmentId);
        return segment != null;
    }

    public boolean checkAnyTargetSegmentExists() {
        List<String> topJobTargetSegments = targetSegments;
        AbstractExecutable parent = getParent();
        if (parent != null) {
            topJobTargetSegments = parent.targetSegments;
        }

        Preconditions.checkState(!topJobTargetSegments.isEmpty());

        return topJobTargetSegments.stream().anyMatch(this::checkTargetSegmentExists);
    }

    private boolean checkAnyLayoutExists() {
        val layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isEmpty(layouts)) {
            return true;
        }
        val cubeManager = NIndexPlanManager.getInstance(config, getProject());
        val cube = cubeManager.getIndexPlan(targetModel);
        val allLayoutIds = cube.getAllLayouts().stream().map(l -> l.getId() + "").collect(Collectors.toSet());
        return Stream.of(StringUtil.splitAndTrim(layouts, ",")).anyMatch(allLayoutIds::contains);
    }

    private void suicideIfNecessary() {
        if (needSuicide()) {
            updateJobOutput(project, getId(), ExecutableState.SUICIDAL,
                    "suicide as its subject model/segment no longer exists");
            throw new JobSuicideException();
        }
    }

    protected boolean needSuicide() {
        return !checkAnyTargetSegmentExists() || checkCuttingInJobByModel() || !checkAnyLayoutExists();
    }

    public boolean checkCuttingInJobByModel() {
        AbstractExecutable parent = getParent();
        if (parent == null) {
            parent = this;
        }
        if (!JobTypeEnum.INDEX_BUILD.equals(parent.getJobType())) {
            return false;
        }
        val model = parent.getTargetModel();
        return NExecutableManager.getInstance(config, getProject()).countCuttingInJobByModel(model, parent) > 0;
    }

    public AbstractExecutable() {
        setId(UUID.randomUUID().toString());
    }

    public void initConfig(KylinConfig config) {
        Preconditions.checkState(this.config == null || this.config == config);
        this.config = config;
    }

    public void cancelJob() throws IOException {
    }

    protected KylinConfig getConfig() {
        return config;
    }

    protected NExecutableManager getManager() {
        return getExecutableManager(project);
    }

    protected void onExecuteStart(ExecutableContext executableContext) {

        suicideIfNecessary();
        checkJobPaused();
        updateJobOutput(project, getId(), ExecutableState.RUNNING);

    }

    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        suicideIfNecessary();

        Preconditions.checkState(result.succeed());
        Preconditions.checkState(this.getStatus() == ExecutableState.RUNNING);
        updateJobOutput(project, getId(), ExecutableState.SUCCEED, result.getExtraInfo(), result.output());
    }

    protected void onExecuteError(ExecuteResult result, ExecutableContext executableContext) {
        suicideIfNecessary();
        checkJobPaused(result.getErrorMsg());

        Preconditions.checkState(!result.succeed());
        Preconditions.checkState(this.getStatus() == ExecutableState.RUNNING);
        updateJobOutput(project, getId(), ExecutableState.ERROR, result.getExtraInfo(), result.getErrorMsg(),
                jobId -> onExecuteErrorHook(jobId));
    }

    protected void onExecuteStopHook() {
        onExecuteErrorHook(getId());
    }

    protected void onExecuteErrorHook(String jobId) {
        if (!(this instanceof DefaultChainedExecutable)) {
            return;
        }
        markDFLagBehindIfNecessary(jobId);
    }

    private void markDFLagBehindIfNecessary(String jobId) {
        if (!JobTypeEnum.INC_BUILD.equals(this.jobType)) {
            return;
        }
        val dataflow = getDataflow(jobId);
        if (dataflow == null || RealizationStatusEnum.LAG_BEHIND.equals(dataflow.getStatus())) {
            return;
        }
        val model = dataflow.getModel();
        if (ManagementType.MODEL_BASED.equals(model.getManagementType())) {
            return;
        }
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        dfManager.updateDataflow(dataflow.getId(),
                copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND));
    }

    private NDataflow getDataflow(String jobId) {
        val execManager = getExecutableManager(getProject());
        val executable = (DefaultChainedExecutable) execManager.getJob(jobId);
        val modelId = executable.getTargetModel();
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        return dfManager.getDataflow(modelId);
    }

    public static void updateJobOutput(String project, String jobId, ExecutableState newStatus,
            Map<String, String> info, String output) {
        updateJobOutput(project, jobId, newStatus, info, output, null);
    }

    public static void updateJobOutput(String project, String jobId, ExecutableState newStatus,
            Map<String, String> info, String output, Consumer<String> hook) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            NExecutableManager executableManager = getExecutableManager(project);
            val existedInfo = executableManager.getOutput(jobId).getExtra();
            if (info != null) {
                existedInfo.putAll(info);
            }

            //The output will be stored in HDFS,not in RS
            executableManager.updateJobOutput(jobId, newStatus, existedInfo, null, null);
            if (hook != null) {
                hook.accept(jobId);
            }
            return null;
        }, project);

        //write output to HDFS
        updateJobOutputToHDFS(project, jobId, output);
    }

    public static void updateJobOutput(String project, String jobId, ExecutableState newStatus) {
        updateJobOutput(project, jobId, newStatus, null);
    }

    public static void updateJobOutput(String project, String jobId, ExecutableState newStatus, String output) {
        updateJobOutput(project, jobId, newStatus, output, null);
    }

    public static void updateJobOutput(String project, String jobId, ExecutableState newStatus, String output,
            Consumer<String> hook) {
        updateJobOutput(project, jobId, newStatus, null, output, hook);
    }

    private static void updateJobOutputToHDFS(String project, String jobId, String output) {
        NExecutableManager nExecutableManager = getExecutableManager(project);
        ExecutableOutputPO jobOutput = nExecutableManager.getJobOutput(jobId);
        if (output != null) {
            jobOutput.setContent(output);
        }

        String outputHDFSPath = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId);

        nExecutableManager.updateJobOutputToHDFS(outputHDFSPath, jobOutput);
    }

    protected static NExecutableManager getExecutableManager(String project) {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {

        logger.info("Executing AbstractExecutable {}", this.getDisplayName());
        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result;
        onExecuteStart(executableContext);

        do {
            if (retry > 0) {
                logger.info("Retrying for the {}th time ", retry);
            }

            suicideIfNecessary();
            checkJobPaused();
            try {
                result = doWork(executableContext);
            } catch (Throwable e) {
                result = ExecuteResult.createError(e);
            }

            retry++;

        } while (needRetry(this.retry, result.getThrowable())); //exception in ExecuteResult should handle by user itself.
        //check exception in result to avoid retry on ChainedExecutable(only need retry on subtask actually)

        if (!result.succeed()) {
            onExecuteError(result, executableContext);
            throw new ExecuteException(result.getThrowable());
        } else {
            onExecuteFinished(result, executableContext);
            return result;
        }
    }

    public void checkJobPaused() {
        checkJobPaused(null);
    }

    public void checkJobPaused(String output) {
        if (this instanceof DefaultChainedExecutable) {
            return;
        }
        val parent = this.getParent();
        try {
            // doInTransaction for get latest status
            UnitOfWork.doInTransactionWithRetry(() -> {
                if (ExecutableState.PAUSED.equals(parent.getStatus())) {
                    throw new JobStoppedException();
                }
                return 0;
            }, project, 1);
        } catch (TransactionException e) {
            if (e.getCause() instanceof JobStoppedException) {
                updateJobOutput(project, getId(), ExecutableState.READY);
                throw (JobStoppedException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    // Retry will happen in below cases:
    // 1) if property "kylin.job.retry-exception-classes" is not set or is null, all jobs with exceptions will retry according to the retry times.
    // 2) if property "kylin.job.retry-exception-classes" is set and is not null, only jobs with the specified exceptions will retry according to the retry times.
    public boolean needRetry(int retry, Throwable t) {
        if (retry > KylinConfig.getInstanceFromEnv().getJobRetry() || t == null
                || (this instanceof DefaultChainedExecutable)) {
            return false;
        } else {
            return isRetryableException(t.getClass().getName());
        }
    }

    private static boolean isRetryableException(String exceptionName) {
        String[] jobRetryExceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        return ArrayUtils.isEmpty(jobRetryExceptions) || ArrayUtils.contains(jobRetryExceptions, exceptionName);
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public void cleanup() {
    }

    @Override
    public boolean isRunnable() {
        return this.getStatus() == ExecutableState.READY;
    }

    public String getDisplayName() {
        return this.name + " (" + this.id + ")";
    }

    @Override
    public final ExecutableState getStatus() {
        NExecutableManager manager = getManager();
        return manager.getOutput(this.getId()).getState();
    }

    @Override
    public final Map<String, String> getParams() {
        return this.params;
    }

    public final String getParam(String key) {
        return this.params.get(key);
    }

    public final void setParam(String key, String value) {
        this.params.put(key, value);
    }

    public final void setParams(Map<String, String> params) {
        this.params.putAll(params);
    }

    public final long getLastModified() {
        return getOutput().getLastModified();
    }

    public final void setParent(AbstractExecutable parent) {
        setParentId(parent.getId());
    }

    public final void setParentId(String parentId) {
        setParam(PARENT_ID, parentId);
    }

    public final void setSubmitter(String submitter) {
        setParam(SUBMITTER, submitter);
    }

    public final List<String> getNotifyList() {
        final String str = getParam(NOTIFY_LIST);
        if (str != null) {
            return Lists.newArrayList(StringUtils.split(str, ","));
        } else {
            return Collections.emptyList();
        }
    }

    public final void setNotifyList(String notifications) {
        setParam(NOTIFY_LIST, notifications);
    }

    public final void setNotifyList(List<String> notifications) {
        setNotifyList(StringUtils.join(notifications, ","));
    }

    protected Pair<String, String> formatNotifications(KylinConfig kylinConfig, EmailNotificationContent content) {
        if (content == null) {
            return null;
        }
        String title = content.getEmailTitle();
        String body = content.getEmailBody();
        return Pair.of(title, body);
    }

    public void notifyUserIfNecessary(NDataLayout[] addOrUpdateCuboids) {
        boolean hasEmptyLayout = false;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            if (dataCuboid.getRows() == 0) {
                hasEmptyLayout = true;
                break;
            }
        }
        if (hasEmptyLayout) {
            notifyUserJobIssue(JobIssueEnum.LOAD_EMPTY_DATA);
        }
    }

    public final void notifyUserJobIssue(JobIssueEnum jobIssue) {
        Preconditions.checkState(
                (this instanceof DefaultChainedExecutable) || this.getParent() instanceof DefaultChainedExecutable);
        val projectConfig = NProjectManager.getInstance(config).getProject(project).getConfig();
        boolean needNotification = true;
        switch (jobIssue) {
        case JOB_ERROR:
            needNotification = projectConfig.getJobErrorNotificationEnabled();
            break;
        case LOAD_EMPTY_DATA:
            needNotification = projectConfig.getJobDataLoadEmptyNotificationEnabled();
            break;
        case SOURCE_RECORDS_CHANGE:
            needNotification = projectConfig.getJobSourceRecordsChangeNotificationEnabled();
            break;
        default:
            throw new IllegalArgumentException(String.format("no process for jobIssue: %s.", jobIssue));
        }
        if (!needNotification) {
            return;
        }
        if (this instanceof DefaultChainedExecutable) {
            notifyUser(projectConfig, EmailNotificationContent.createContent(jobIssue, this));
        } else {
            notifyUser(projectConfig, EmailNotificationContent.createContent(jobIssue, this.getParent()));
        }
    }

    private final void notifyUser(KylinConfig kylinConfig, EmailNotificationContent content) {
        try {
            List<String> users = getAllNofifyUsers(kylinConfig);
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty.");
                return;
            }
            final Pair<String, String> email = formatNotifications(kylinConfig, content);
            doSendMail(kylinConfig, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    private List<String> getAllNofifyUsers(KylinConfig kylinConfig) {
        List<String> users = Lists.newArrayList();
        users.addAll(getNotifyList());
        final String[] adminDls = kylinConfig.getAdminDls();
        if (null != adminDls) {
            for (String adminDl : adminDls) {
                users.add(adminDl);
            }
        }
        return users;
    }

    private void doSendMail(KylinConfig kylinConfig, List<String> users, Pair<String, String> email) {
        if (email == null) {
            logger.warn("no need to send email, content is null");
            return;
        }
        logger.info("prepare to send email to:" + users);
        logger.info("job name:" + getDisplayName());
        logger.info("submitter:" + getSubmitter());
        logger.info("notify list:" + users);
        new MailService(kylinConfig).sendMail(users, email.getLeft(), email.getRight());
    }

    protected void sendMail(Pair<String, String> email) {
        try {
            List<String> users = getAllNofifyUsers(config);
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty");
                return;
            }
            doSendMail(config, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    public final String getParentId() {
        return getParam(PARENT_ID);
    }

    public final AbstractExecutable getParent() {
        return getManager().getJob(getParam(PARENT_ID));
    }

    public final String getSubmitter() {
        return getParam(SUBMITTER);
    }

    public final String getProject() {
        if (project == null) {
            throw new IllegalStateException("project is not set for abstract executable " + getId());
        }
        return project;
    }

    public final void setProject(String project) {
        this.project = project;
    }

    @Override
    public final Output getOutput() {
        return getManager().getOutput(getId());
    }

    //will modify input info
    public Map<String, String> makeExtraInfo(Map<String, String> info) {
        if (info == null) {
            return Maps.newHashMap();
        }

        // post process
        if (info.containsKey(MR_JOB_ID) && !info.containsKey(YARN_APP_ID)) {
            String jobId = info.get(MR_JOB_ID);
            if (jobId.startsWith("job_")) {
                info.put(YARN_APP_ID, jobId.replace("job_", "application_"));
            }
        }

        if (info.containsKey(YARN_APP_ID)
                && !org.apache.commons.lang3.StringUtils.isEmpty(config.getJobTrackingURLPattern())) {
            String pattern = config.getJobTrackingURLPattern();
            try {
                String newTrackingURL = String.format(pattern, info.get(YARN_APP_ID));
                info.put(YARN_APP_URL, newTrackingURL);
            } catch (IllegalFormatException ife) {
                logger.error("Illegal tracking url pattern: {}", config.getJobTrackingURLPattern());
            }
        }

        return info;
    }

    public static long getStartTime(Output output) {
        return output.getStartTime();
    }

    public static long getEndTime(Output output) {
        return output.getEndTime();
    }

    protected final Map<String, String> getExtraInfo() {
        return getOutput().getExtra();
    }

    public final long getStartTime() {
        return getStartTime(getOutput());
    }

    public final long getCreateTime() {
        return getManager().getCreateTime(getId());
    }

    public static final long getCreateTime(Output output) {
        return output.getCreateTime();
    }

    public final long getEndTime() {
        return getEndTime(getOutput());
    }

    public final long getDuration() {
        return getDuration(getOutput());
    }

    public static final long getDuration(Output output) {
        return getDuration(output.getStartTime(), output.getEndTime(), output.getWaitTime());
    }

    public static long getDuration(long startTime, long endTime, long interruptTime) {
        if (startTime == 0) {
            return 0;
        }
        if (endTime == 0) {
            return System.currentTimeMillis() - startTime - interruptTime;
        } else {
            return endTime - startTime - interruptTime;
        }
    }

    public final long getWaitTime() {
        Output output = getOutput();
        long pendingDuration = 0L;
        if (this instanceof DefaultChainedExecutable) {
            long startTime = output.getStartTime();
            if (startTime == 0) {
                return System.currentTimeMillis() - output.getCreateTime();
            }
            pendingDuration += (startTime - output.getCreateTime());
        }
        pendingDuration += output.getWaitTime();
        if (output.getEndTime() > 0 && !output.getState().isFinalState()) {
            pendingDuration += System.currentTimeMillis() - output.getEndTime();
        }
        return pendingDuration;
    }

    public final Set<String> getDependentFiles() {
        val value = getExtraInfo().getOrDefault(DEPENDENT_FILES, "");
        if (StringUtils.isEmpty(value)) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(value.split(","));
    }

    /*
    * discarded is triggered by JobService, the Scheduler is not awake of that
    *
    * */
    protected final boolean isDiscarded() {
        final ExecutableState status = getOutput().getState();
        return status == ExecutableState.DISCARDED;
    }

    protected final boolean isPaused() {
        final ExecutableState status = getOutput().getState();
        return status == ExecutableState.PAUSED;
    }

    protected boolean needRetry() {
        return this.retry <= config.getJobRetry();
    }

    public Set<String> getDependencies(KylinConfig config) {
        return Sets.newHashSet();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus())
                .toString();
    }

}
