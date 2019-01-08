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
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import lombok.val;

abstract class AbstractEventWithJobHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEventWithJobHandler.class);

    @Override
    protected final void doHandle(EventContext eventContext) {
        super.doHandle(eventContext);

        Event event = eventContext.getEvent();
        val project = eventContext.getProject();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val eventId = event.getId();
        AbstractExecutable job = createJob(eventContext.getEvent(), project);
        if (job == null) {
            logger.info("No job is required by event {}, aborting handler...", event);
            finishEvent(project, eventId);
            return;
        }

        job.initConfig(kylinConfig);
        val po = NExecutableManager.toPO(job, project);

        NExecutableManager executableManager = getExecutableManager(project, kylinConfig);
        executableManager.addJob(po);

        finishEvent(project, eventId);

    }

    /**
     * createJob is wrapped by UnitOfWork
     */
    protected abstract AbstractExecutable createJob(Event event, String project);

}
