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
package io.kylingence.kap.event.handle;



import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.EventStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;

import io.kylingence.kap.event.model.EventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


abstract class AbstractEventWithJobHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEventWithJobHandler.class);

    @Override
    protected final void doHandle(EventContext eventContext) throws Exception {
        Event event = eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();
        NExecutableManager execMgr = NExecutableManager.getInstance(kylinConfig, project);

        AbstractExecutable job;
        if (event.getJobId() == null) {
            job = createJob(eventContext);
            if (job == null) {
                return;
            }
            execMgr.addJob(job);
            eventContext.getEvent().setJobId(job.getId());
            getEventDao(eventContext).updateEvent(eventContext.getEvent());
        } else {
            job = NExecutableManager.getInstance(kylinConfig, project).getJob(event.getJobId());
        }

        waitForJobFinished(job, eventContext, false);

        ExecutableState jobStatus = job.getStatus();
        eventContext.setJobStatus(jobStatus);
        if (ExecutableState.SUCCEED.equals(jobStatus)) {
            onJobSuccess(eventContext);
        } else if (ExecutableState.ERROR.equals(jobStatus)) {
            if (needRetryJob(job, eventContext)) {
                NExecutableManager.getInstance(kylinConfig, project).resumeJob(job.getId());
            } else {
                onJobError(eventContext);
            }
        } else if (ExecutableState.DISCARDED.equals(jobStatus)) {
            onJobDiscarded(eventContext);
        }
    }

    private boolean needRetryJob(AbstractExecutable job, EventContext eventContext) throws PersistentException {
        Event event = eventContext.getEvent();
        int jobRetry = event.getJobRetry();
        if (jobRetry > 0 && job != null) {
            jobRetry --;
            event.setJobRetry(jobRetry);
            getEventDao(eventContext).updateEvent(event);
            return true;
        }
        return false;
    }

    @Override
    protected void onHandleFinished(EventContext eventContext) throws Exception {
        Event event = eventContext.getEvent();
        String jobId = event.getJobId();
        if (StringUtils.isNotBlank(jobId)) {
            ExecutableState jobStatus = eventContext.getJobStatus();
            if (ExecutableState.SUCCEED.equals(jobStatus) || ExecutableState.DISCARDED.equals(jobStatus)) {
                // event stats is running unless the job stats is succeed or discard
                // no need update event if stats is still running
                event.setStatus(EventStatus.SUCCEED);
                getEventDao(eventContext).updateEvent(event);
            }
        }
    }

    protected void onJobError(EventContext eventContext) throws Exception {}

    protected void onJobSuccess(EventContext eventContext) throws Exception {}

    protected void onJobDiscarded(EventContext eventContext) throws Exception {}

    protected abstract AbstractExecutable createJob(EventContext eventContext) throws Exception;

    protected void waitForJobFinished(AbstractExecutable job, EventContext eventContext, boolean wait) throws Exception {
        while (wait) {
            ExecutableState jobStatus = job.getStatus();
            if (jobStatus == ExecutableState.SUCCEED ||
                    jobStatus == ExecutableState.ERROR || // if job failed, the event will not block, and the stats of
                    // event is still running(waiting), so that other model's event will not be blocked.
                    jobStatus == ExecutableState.DISCARDED) {
                break;
            } else {
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    logger.error("waitForJobStatus error : " + e.getMessage(), e);
                }
            }
        }
    }

    protected void updateDataLoadingRange(NDataflow df) throws IOException {
        NDataModel model = df.getModel();
        String tableName = model.getRootFactTableName();
        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(df.getConfig(), df.getProject());
        dataLoadingRangeManager.updateDataLoadingRangeWaterMark(tableName);
    }
}
