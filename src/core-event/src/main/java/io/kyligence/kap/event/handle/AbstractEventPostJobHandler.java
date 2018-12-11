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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.JobRelatedEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEventPostJobHandler extends AbstractEventHandler {
    @Override
    protected void doHandle(EventContext eventContext) {
        val event = (JobRelatedEvent) eventContext.getEvent();
        String project = event.getProject();
        val id = event.getId();
        val jobId = event.getJobId();

        val execManager = getExecutableManager(project, eventContext.getConfig());
        val executable = (ChainedExecutable) execManager.getJob(jobId);
        if (executable == null) {
            log.info("no job created, abort handler");
            UnitOfWork.doInTransactionWithRetry(() -> {
                EventDao eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
                eventDao.deleteEvent(id);
                return null;
            }, project);
            return;
        }
        doHandle(eventContext, executable);
    }

    protected abstract void doHandle(EventContext eventContext, ChainedExecutable executable);
}
