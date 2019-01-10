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
package io.kyligence.kap.event.handle;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;

import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.JobRelatedEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.TimeZone;

@Slf4j
public abstract class AbstractEventPostJobHandler extends AbstractEventHandler {
    @Override
    protected void doHandle(EventContext eventContext) {
        super.doHandle(eventContext);

        val event = (JobRelatedEvent) eventContext.getEvent();
        String project = eventContext.getProject();
        val jobId = event.getJobId();

        val execManager = getExecutableManager(project, eventContext.getConfig());
        val executable = (ChainedExecutable) execManager.getJob(jobId);
        if (executable == null) {
            log.debug("executable is null when handling event {}", eventContext.getEvent());
            // in case the job is skipped
            doHandleWithNullJob(eventContext);
            return;
        } else if (executable.getStatus() == ExecutableState.SUICIDAL) {
            restartNewJobIfNecessary(eventContext, executable);
            log.debug("previous job suicide, current event:{} will be ignored", eventContext.getEvent());
            finishEvent(eventContext.getProject(), eventContext.getEvent().getId());
            return;
        }
        doHandle(eventContext, executable);
    }

    protected void restartNewJobIfNecessary(EventContext eventContext, ChainedExecutable executable) {
    }

    protected abstract void doHandleWithNullJob(EventContext eventContext);

    protected void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids) {
        String model = buildTask.getTargetModel();
        long buildEndTime = buildTask.getParent().getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = 0;

        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            byteSize += dataCuboid.getByteSize();
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ZoneId zoneId = TimeZone.getTimeZone(kylinConfig.getTimeZone()).toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(buildEndTime).atZone(zoneId).toLocalDate();
        long startOfDay = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();
        // update
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig, buildTask.getProject());
        jobStatisticsManager.updateStatistics(startOfDay, model, duration, byteSize);
    }

    /**
     *
     * @param eventContext
     * @param executable
     */
    protected abstract void doHandle(EventContext eventContext, ChainedExecutable executable);
}
