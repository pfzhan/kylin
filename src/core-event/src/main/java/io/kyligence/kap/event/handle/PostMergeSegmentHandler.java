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

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.merger.AfterMergeResourceMerger;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostMergeSegmentEvent;
import lombok.val;

public class PostMergeSegmentHandler extends AbstractEventPostJobHandler {
    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        val event = (PostMergeSegmentEvent) eventContext.getEvent();
        String project = event.getProject();
        val id = event.getId();
        val jobId = event.getJobId();

        val tasks = executable.getTasks();
        Preconditions.checkState(tasks.size() > 0, "job " + jobId + " steps is not enough");
        val task = tasks.get(0);
        val dataflowName = ExecutableUtils.getDataflowName(task);
        val segmentIds = ExecutableUtils.getSegmentIds(task);
        val buildResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), task);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val merger = new AfterMergeResourceMerger(kylinConfig, project);
            merger.mergeAfterJob(dataflowName, segmentIds.iterator().next(), buildResourceStore);

            val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
            eventDao.deleteEvent(id);

            return null;
        }, project);
    }
}
