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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.metrics.NMetricsTag;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultChainedExecutable() {
        super();
    }

    public Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        for (Executable subTask : executables) {
            if (subTask.isRunnable()) {
                subTask.execute(context);
            } else if (ExecutableState.SUCCEED.equals(subTask.getStatus())) {
                logger.info("step {} is already succeed, skip it.", subTask.getDisplayName());
            } else {
                throw new IllegalStateException("invalid subtask state, sub task:" + subTask.getDisplayName()
                        + ", state:" + subTask.getStatus());
            }
        }
        return ExecuteResult.createSucceed();

    }

    @Override
    protected boolean needCheckState() {
        return false;
    }

    @Override
    protected void onExecuteStart() throws JobStoppedException {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (isStoppedNonVoluntarily() && //
            !ExecutableState.READY.equals(getOutput().getState())) //onExecuteStart will turn READY to RUNNING
                return null;

            updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
            return null;
        }, project);
    }

    @Override
    protected void onExecuteError(ExecuteResult result) throws JobStoppedException {
        Preconditions.checkState(!result.succeed());

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (isStoppedNonVoluntarily())
                return null;

            updateJobOutput(project, getId(), ExecutableState.ERROR, result.getExtraInfo(), result.getErrorMsg(),
                    this::onExecuteErrorHook);
            return null;
        }, project);

        notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
        NMetricsGroup.counterInc(NMetricsName.JOB_ERROR, NMetricsCategory.PROJECT, getProject());
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {

        Preconditions.checkState(result.succeed());

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            boolean hasError = false;
            boolean hasDiscarded = false;
            boolean hasSuicidal = false;
            boolean hasPaused = false;
            for (Executable task : jobs) {
                boolean taskSucceed = false;
                switch (task.getStatus()) {
                case RUNNING:
                    hasError = true;
                    break;
                case ERROR:
                    hasError = true;
                    break;
                case DISCARDED:
                    hasDiscarded = true;
                    break;
                case SUICIDAL:
                    hasSuicidal = true;
                    break;
                case PAUSED:
                    hasPaused = true;
                    break;
                case SUCCEED:
                    taskSucceed = true;
                    break;
                default:
                    break;
                }
                allSucceed &= taskSucceed;
            }
            if (allSucceed) {
                updateToFinalState(ExecutableState.SUCCEED, this::afterUpdateOutput);
            } else if (hasDiscarded) {
                updateToFinalState(ExecutableState.DISCARDED, this::onExecuteDiscardHook);
            } else if (hasSuicidal) {
                updateToFinalState(ExecutableState.SUICIDAL, this::onExecuteSuicidalHook);
            } else {
                if (isStoppedNonVoluntarily())
                    return null;
                if (hasError) {
                    logger.warn("[UNEXPECTED_THINGS_HAPPENED] Unexpected ERROR state discovered here!!!");
                    updateJobOutput(getProject(), getId(), ExecutableState.ERROR, null, null, this::onExecuteErrorHook);
                    notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
                } else if (hasPaused) {
                    updateJobOutput(getProject(), getId(), ExecutableState.PAUSED, null, null, null);
                } else {
                    //restart case
                    updateJobOutput(getProject(), getId(), ExecutableState.READY, null, null, null);
                }
            }

            return null;

        }, project);

        // dispatch job-finished message out
        SchedulerEventBusFactory.getInstance(KylinConfig.getInstanceFromEnv())
                .postWithLimit(new JobFinishedNotifier(getProject()));

        updateMetrics();

    }

    protected void onExecuteDiscardHook(String jobId) {
        // Hook method, default action is doing nothing
    }

    protected void onExecuteSuicidalHook(String jobId) {
        // Hook method, default action is doing nothing
    }

    private void updateToFinalState(ExecutableState finalState, Consumer<String> hook) {
        //to final state, regardless of isStoppedNonVoluntarily, otherwise a paused job might fail to suicide
        if (!getOutput().getState().isFinalState()) {
            updateJobOutput(getProject(), getId(), finalState, null, null, hook);
        }
    }

    private void updateMetrics() {
        ExecutableState state = getStatus();
        if (state != null && state.isFinalState()) {
            NMetricsGroup.counterInc(NMetricsName.JOB_FINISHED, NMetricsCategory.PROJECT, getProject());
            NMetricsGroup.counterInc(NMetricsName.JOB_DURATION, NMetricsCategory.PROJECT, getProject(), getDuration());
            NMetricsGroup.histogramUpdate(NMetricsName.JOB_DURATION_HISTOGRAM, NMetricsCategory.PROJECT, getProject(),
                    getDuration());
            NMetricsGroup.counterInc(NMetricsName.JOB_WAIT_DURATION, NMetricsCategory.PROJECT, getProject(),
                    getWaitTime());

            String modelAlias = getTargetModelAlias();
            if (modelAlias != null) {
                Map<String, String> tags = Maps.newHashMap();
                tags.put(NMetricsTag.MODEL.getVal(), project.concat("-").concat(modelAlias));
                NMetricsGroup.counterInc(NMetricsName.MODEL_BUILD_DURATION, NMetricsCategory.PROJECT, getProject(),
                        tags, getDuration());
                NMetricsGroup.counterInc(NMetricsName.MODEL_WAIT_DURATION, NMetricsCategory.PROJECT, getProject(), tags,
                        getWaitTime());
                NMetricsGroup.histogramUpdate(NMetricsName.MODEL_BUILD_DURATION_HISTOGRAM, NMetricsCategory.PROJECT, project,
                        tags, getDuration());
            }
        }
    }

    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    @Override
    protected boolean needRetry() {
        return false;
    }

    @Override
    public void addTask(AbstractExecutable executable) {
        executable.setId(getId() + "_" + String.format("%02d", subTasks.size()));
        executable.setParent(this);
        this.subTasks.add(executable);
    }

    @Override
    public <T extends AbstractExecutable> T getTask(Class<T> clz) {
        List<AbstractExecutable> tasks = getTasks();
        for (AbstractExecutable task : tasks) {
            if (task.getClass().equals(clz)) {
                return (T) task;
            }
        }
        return null;
    }

    protected void afterUpdateOutput(String jobId) {
        // just implement it
    }

}
