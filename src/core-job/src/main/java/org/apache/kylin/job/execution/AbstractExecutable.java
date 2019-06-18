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
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.exception.JobStoppedVoluntarilyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.ThrowableUtils;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.base.Throwables;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.experimental.Delegate;

/**
 */
public abstract class AbstractExecutable implements Executable {

    public interface Callback {
        void process() throws Exception;

        default void onProcessError(Throwable throwable) {
        }
    }

    protected static final String SUBMITTER = "submitter";
    protected static final String NOTIFY_LIST = "notify_list";
    protected static final String PARENT_ID = "parentId";
    public static final String RUNTIME_INFO = "runtimeInfo";
    public static final String DEPENDENT_FILES = "dependentFiles";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);

    protected int retry = 0;

    @Getter
    @Setter
    private String name;
    @Getter
    @Setter
    private JobTypeEnum jobType;

    @Setter
    @Getter
    private String targetSubject; //uuid of the model or table identity if table sampling

    @Setter
    @Getter
    private List<String> targetSegments = Lists.newArrayList();//uuid of related segments

    @Getter
    @Setter
    private String id;

    @Delegate
    private ExecutableParams executableParams = new ExecutableParams();
    private String project;
    private ExecutableContext context;

    @Getter
    @Setter
    private Map<String, Object> runTimeInfo = Maps.newHashMap();

    public String getTargetModelAlias() {
        val modelManager = NDataModelManager.getInstance(getConfig(), getProject());
        NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), getProject())
                .getDataModelDesc(targetSubject);
        return dataModelDesc == null ? null
                : modelManager.isModelBroken(targetSubject)
                        ? modelManager.getDataModelDescWithoutInit(targetSubject).getAlias()
                        : dataModelDesc.getAlias();

    }

    public String getTargetSubjectAlias() {
        return getTargetModelAlias();
    }

    public AbstractExecutable() {
        setId(UUID.randomUUID().toString());
    }

    public void cancelJob() throws IOException {
    }

    protected KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    protected NExecutableManager getManager() {
        return getExecutableManager(project);
    }

    protected void wrapWithCheckQuit(Callback f) throws JobStoppedException {
        boolean tryAgain = true;

        while (tryAgain) {
            checkNeedQuit(true);

            // in this short period user might changed job state

            tryAgain = false;
            try {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    checkNeedQuit(false);
                    f.process();
                    return null;
                }, project);
            } catch (Exception e) {
                if (Throwables.getCausalChain(e).stream().anyMatch(x -> x instanceof JobStoppedException)) {
                    // "in this short period user might changed job state" happens
                    logger.info("[LESS_LIKELY_THINGS_HAPPENED] JobStoppedException thrown from in a UnitOfWork", e);
                    tryAgain = true;
                }
            }
        }
    }

    protected final void onExecuteStart() throws JobStoppedException {
        wrapWithCheckQuit(() -> {
            updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
        });
    }

    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {
        Preconditions.checkState(result.succeed());

        wrapWithCheckQuit(() -> {
            updateJobOutput(project, getId(), ExecutableState.SUCCEED, result.getExtraInfo(), result.output(), null);
        });
    }

    protected void onExecuteError(ExecuteResult result) throws JobStoppedException {
        Preconditions.checkState(!result.succeed());

        wrapWithCheckQuit(() -> {
            updateJobOutput(project, getId(), ExecutableState.ERROR, result.getExtraInfo(), result.getErrorMsg(),
                    this::onExecuteErrorHook);
        });
    }

    protected void onExecuteStopHook() {
        onExecuteErrorHook(getId());
    }

    protected void onExecuteErrorHook(String jobId) {
        // At present, only instance of DefaultChainedExecutableOnModel take full advantage of this method.
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, Consumer<String> hook) {
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
        this.context = executableContext;
        ExecuteResult result;

        onExecuteStart();

        do {
            if (retry > 0) {
                pauseOnRetry();
                logger.info("Retrying for the {}th time ", retry);
            }

            checkNeedQuit(true);
            try {
                result = doWork(executableContext);
            } catch (JobStoppedException jse) {
                // job quits voluntarily or non-voluntarily, in this case, the job is "finished"
                // we createSucceed() to run onExecuteFinished()
                result = ExecuteResult.createSucceed();
            } catch (Throwable e) {
                result = ExecuteResult.createError(e);
            }

            retry++;

        } while (needRetry(this.retry, result.getThrowable())); //exception in ExecuteResult should handle by user itself.
        //check exception in result to avoid retry on ChainedExecutable(only need retry on subtask actually)

        if (!result.succeed()) {
            onExecuteError(result);
            throw new ExecuteException(result.getThrowable());
        } else {
            onExecuteFinished(result);
            return result;
        }
    }

    protected void checkNeedQuit(boolean applyChange) throws JobStoppedException {
        // voluntarily
        suicideIfNecessary(applyChange);
        // non voluntarily
        abortIfJobStopped(applyChange);
    }

    private void suicideIfNecessary(boolean applyChange) throws JobStoppedException {
        if (checkSuicide()) {
            if (applyChange) {
                updateJobOutput(project, getId(), ExecutableState.SUICIDAL, null,
                        "suicide as its subject(model, segment or table) no longer exists", null);
            }
            throw new JobStoppedVoluntarilyException();
        }
    }

    /**
     * For non-chained executable, depend on its parent(instance of DefaultChainedExecutable).
     */
    public boolean checkSuicide() {
        final AbstractExecutable parent = getParent();
        if (parent == null) {
            return false;
        } else {
            return parent.checkSuicide();
        }
    }

    // If job need check external status change, override this method, by default return true.
    protected boolean needCheckState() {
        return true;
    }

    /**
     * will throw exception if necessary!
     */
    public void abortIfJobStopped(boolean applyChange) throws JobStoppedException {
        if (!needCheckState()) {
            return;
        }

        val parent = this.getParent();

        if (ExecutableState.READY.equals(parent.getStatus())) {
            //if a job is restarted(all steps' status changed to READY), the old thread may still be alive and attempt to update job output
            //in this case the old thread should fail itself by calling this
            if (applyChange) {
                updateJobOutput(project, getId(), ExecutableState.READY, null, null, null);
            }
            throw new JobStoppedNonVoluntarilyException();
        } else if (ExecutableState.PAUSED.equals(parent.getStatus())) {
            //if a job is paused
            if (applyChange) {
                updateJobOutput(project, getId(), ExecutableState.READY, null, null, null);
            }
            throw new JobStoppedNonVoluntarilyException();
        } else if (ExecutableState.DISCARDED.equals(parent.getStatus())) {
            //if a job is discarded
            if (applyChange) {
                updateJobOutput(project, getId(), ExecutableState.DISCARDED, null, null, null);
            }
            throw new JobStoppedNonVoluntarilyException();
        }
    }

    // Retry will happen in below cases:
    // 1) if property "kylin.job.retry-exception-classes" is not set or is null, all jobs with exceptions will retry according to the retry times.
    // 2) if property "kylin.job.retry-exception-classes" is set and is not null, only jobs with the specified exceptions will retry according to the retry times.
    public boolean needRetry(int retry, Throwable t) {
        if (t == null || this instanceof DefaultChainedExecutable)
            return false;

        if (retry > KylinConfig.getInstanceFromEnv().getJobRetry())
            return false;

        if (ThrowableUtils.isInterruptedException(t))
            return false;

        return isRetryableException(t.getClass().getName());
    }

    // pauseOnRetry should only works when retry has been triggered
    private void pauseOnRetry() {
        int interval = KylinConfig.getInstanceFromEnv().getJobRetryInterval();
        logger.info("Pause {} milliseconds before retry", interval);
        try {
            TimeUnit.MILLISECONDS.sleep(interval);
        } catch (InterruptedException e) {
            logger.error("Job retry was interrupted, details: {}", e);
            Thread.currentThread().interrupt();
        }
    }

    private static boolean isRetryableException(String exceptionName) {
        String[] jobRetryExceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        return ArrayUtils.isEmpty(jobRetryExceptions) || ArrayUtils.contains(jobRetryExceptions, exceptionName);
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

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

    public final long getLastModified() {
        return getOutput().getLastModified();
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
        val projectConfig = NProjectManager.getInstance(getConfig()).getProject(project).getConfig();
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

    public final AbstractExecutable getParent() {
        return getManager().getJob(getParam(PARENT_ID));
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
                && !org.apache.commons.lang3.StringUtils.isEmpty(getConfig().getJobTrackingURLPattern())) {
            String pattern = getConfig().getJobTrackingURLPattern();
            try {
                String newTrackingURL = String.format(pattern, info.get(YARN_APP_ID));
                info.put(YARN_APP_URL, newTrackingURL);
            } catch (IllegalFormatException ife) {
                logger.error("Illegal tracking url pattern: {}", getConfig().getJobTrackingURLPattern());
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

    public static long getCreateTime(Output output) {
        return output.getCreateTime();
    }

    public final long getEndTime() {
        return getEndTime(getOutput());
    }

    public final long getDuration() {
        return getDuration(getOutput());
    }

    public static long getDuration(Output output) {
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

    /**
     * job get DISCARD or PAUSE without job fetcher's awareness
     *
     * SUICIDE is not the case, as it is awared by job fetcher
     *
     */
    protected final boolean isStoppedNonVoluntarily() {
        final ExecutableState status = getOutput().getState();
        return status.isStoppedNonVoluntarily();
    }

    protected boolean needRetry() {
        return this.retry <= getConfig().getJobRetry();
    }

    public Set<String> getDependencies(KylinConfig config) {
        return Sets.newHashSet();
    }

    private static int computeTableAnalyzeMemory() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config.getSparkEngineDriverMemoryTableSampling();
    }

    public int computeStepDriverMemory() {
        if (getJobType() == JobTypeEnum.TABLE_SAMPLING) {
            return computeTableAnalyzeMemory();
        }

        String layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (layouts != null) {
            return computeDriverMemory(StringUtil.splitAndTrim(layouts, ",").length);
        }
        return 0;
    }

    public static Integer computeDriverMemory(int cuboidNum) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int driverMemoryGrowth = config.getSparkEngineDriverMemoryGrowth();
        int driverMemoryMaximum = config.getSparkEngineDriverMemoryMaximum();
        int driverMemoryBase = config.getSparkEngineDriverMemoryBase();
        driverMemoryBase += (cuboidNum / 1000) * driverMemoryGrowth;
        return Math.min(driverMemoryBase, driverMemoryMaximum);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus())
                .toString();
    }

    private final void notifyUser(KylinConfig kylinConfig, EmailNotificationContent content) {
        try {
            List<String> users = getAllNofifyUsers(kylinConfig);
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty.");
                return;
            }
            final Pair<String, String> email = formatNotifications(content);
            doSendMail(kylinConfig, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    private void doSendMail(KylinConfig kylinConfig, List<String> users, Pair<String, String> email) {
        if (email == null) {
            logger.warn("no need to send email, content is null");
            return;
        }
        logger.info("prepare to send email to:{}", users);
        logger.info("job name:{}", getDisplayName());
        logger.info("submitter:{}", getSubmitter());
        logger.info("notify list:{}", users);
        new MailService(kylinConfig).sendMail(users, email.getFirst(), email.getSecond());
    }

    protected void sendMail(Pair<String, String> email) {
        try {
            List<String> users = getAllNofifyUsers(getConfig());
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty");
                return;
            }
            doSendMail(getConfig(), users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }
}
