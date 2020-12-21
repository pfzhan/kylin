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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;

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
            } else if (ExecutableState.SUCCEED == subTask.getStatus()) {
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

            if (isStoppedNonVoluntarily() && ExecutableState.READY != getOutput().getState()) //onExecuteStart will turn READY to RUNNING
                return null;

            updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
            return null;
        }, project);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {
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

        ExecutableState state;
        if (allSucceed) {
            state = ExecutableState.SUCCEED;
        } else if (hasDiscarded) {
            state = ExecutableState.DISCARDED;
        } else if (hasSuicidal) {
            state = ExecutableState.SUICIDAL;
        } else if (hasError) {
            state = ExecutableState.ERROR;
        } else if (hasPaused) {
            state = ExecutableState.PAUSED;
        } else {
            state = ExecutableState.READY;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            switch (state) {
            case SUCCEED:
                updateToFinalState(ExecutableState.SUCCEED, this::afterUpdateOutput);
                break;
            case DISCARDED:
                updateToFinalState(ExecutableState.DISCARDED, this::onExecuteDiscardHook);
                break;
            case SUICIDAL:
                updateToFinalState(ExecutableState.SUICIDAL, this::onExecuteSuicidalHook);
                break;
            case ERROR:
            case PAUSED:
            case READY:
                if (isStoppedNonVoluntarily()) {
                    return null;
                }
                Consumer<String> hook = null;
                Map<String, String> info = null;
                String output = null;
                if (state == ExecutableState.ERROR) {
                    logger.warn("[UNEXPECTED_THINGS_HAPPENED] Unexpected ERROR state discovered here!!!");
                    notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
                    MetricsGroup.hostTagCounterInc(MetricsName.JOB_ERROR, MetricsCategory.PROJECT, getProject());
                    info = result.getExtraInfo();
                    output = result.getErrorMsg();
                    hook = this::onExecuteErrorHook;
                }
                updateJobOutput(getProject(), getId(), state, info, output, hook);
                break;
            default:
                throw new IllegalArgumentException("Illegal state when job finished: " + state);
            }
            return null;

        }, project);

        // dispatch job-finished message out
        EventBusFactory.getInstance().postWithLimit(new JobFinishedNotifier(getId(), getProject(), getTargetSubject(),
                getDuration(), state.toString(), getJobType().toString(), this.getSegmentIds(), this.getLayoutIds()));
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
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_FINISHED, MetricsCategory.PROJECT, getProject());
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_DURATION, MetricsCategory.PROJECT, getProject(),
                    getDuration());
            MetricsGroup.hostTagHistogramUpdate(MetricsName.JOB_DURATION_HISTOGRAM, MetricsCategory.PROJECT,
                    getProject(), getDuration());
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_WAIT_DURATION, MetricsCategory.PROJECT, getProject(),
                    getWaitTime());

            String modelAlias = getTargetModelAlias();
            if (modelAlias != null) {
                Map<String, String> tags = Maps.newHashMap();
                tags.put(MetricsTag.MODEL.getVal(), project.concat("-").concat(modelAlias));
                MetricsGroup.counterInc(MetricsName.MODEL_BUILD_DURATION, MetricsCategory.PROJECT, getProject(), tags,
                        getDuration());
                MetricsGroup.counterInc(MetricsName.MODEL_WAIT_DURATION, MetricsCategory.PROJECT, getProject(), tags,
                        getWaitTime());
                MetricsGroup.histogramUpdate(MetricsName.MODEL_BUILD_DURATION_HISTOGRAM, MetricsCategory.PROJECT,
                        project, tags, getDuration());
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
        executable.setId(getId() + "_" + String.format(Locale.ROOT, "%02d", subTasks.size()));
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
