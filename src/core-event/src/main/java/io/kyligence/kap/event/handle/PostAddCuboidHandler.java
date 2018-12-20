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

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostAddCuboidHandler extends AbstractEventPostJobHandler {

    private static FavoriteQueryManager getFavoriteQueryDao(String project) {
        return FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        String project = eventContext.getProject();
        if (executable == null) {
            log.debug("executable is null when handling event {}", eventContext.getEvent());
            // in case the job is skipped
            doHandleWithNullJob(eventContext);
            return;
        } else if (executable.getStatus() == ExecutableState.DISCARDED) {
            log.debug("previous job suicide, current event:{} will be ignored", eventContext.getEvent());
            UnitOfWork.doInTransactionWithRetry(() -> {
                finishEvent(eventContext.getProject(), eventContext.getEvent().getId());
                return null;
            }, project);
            return;
        }

        PostAddCuboidEvent event = (PostAddCuboidEvent) eventContext.getEvent();
        List<String> sqlList = event.getSqlPatterns();
        val jobId = event.getJobId();

        val tasks = executable.getTasks();
        Preconditions.checkState(tasks.size() > 1, "job " + jobId + " steps is not enough");
        val buildTask = tasks.get(1);
        val dataflowName = ExecutableUtils.getDataflowName(buildTask);
        val segmentIds = ExecutableUtils.getSegmentIds(buildTask);
        val layoutIds = ExecutableUtils.getLayoutIds(buildTask);

        val analysisResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), tasks.get(0));
        val buildResourceStore = ExecutableUtils.getRemoteStore(eventContext.getConfig(), buildTask);
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                if (!checkSubjectExists(project, event.getCubePlanName(), null, event)) {
                    finishEvent(project, event.getId());
                    return null;
                }

                val kylinConfig = KylinConfig.getInstanceFromEnv();
                val merger = new AfterBuildResourceMerger(kylinConfig, project);
                val addedCuboids = merger.mergeAfterCatchup(dataflowName, segmentIds, layoutIds, buildResourceStore);
                merger.mergeAnalysis(dataflowName, analysisResourceStore);

                recordDownJobStats(buildTask, addedCuboids);

                handleFavoriteQuery(project, sqlList);

                finishEvent(project, event.getId());
                return null;
            }, project);
        } finally {
            analysisResourceStore.close();
            buildResourceStore.close();
        }
    }

    private void doHandleWithNullJob(EventContext eventContext) {

        PostAddCuboidEvent event = (PostAddCuboidEvent) eventContext.getEvent();
        String project = eventContext.getProject();
        List<String> sqlList = event.getSqlPatterns();

        UnitOfWork.doInTransactionWithRetry(() -> {
            if (!checkSubjectExists(project, event.getCubePlanName(), null, event)) {
                finishEvent(project, event.getId());
                return null;
            }

            handleFavoriteQuery(project, sqlList);

            finishEvent(project, event.getId());

            return null;
        }, project);
    }

    private void handleFavoriteQuery(String project, List<String> sqlList) {
        if (CollectionUtils.isNotEmpty(sqlList)) {
            for (String sqlPattern : sqlList) {
                getFavoriteQueryDao(project).updateStatus(sqlPattern, FavoriteQueryStatusEnum.FULLY_ACCELERATED, null);
            }
        }

    }

}