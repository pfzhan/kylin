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
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.engine.spark.job.NSparkExecutable;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostAddCuboidHandler extends AbstractEventPostJobHandler {
    private static final Logger logger = LoggerFactory.getLogger(PostAddCuboidHandler.class);

    private static FavoriteQueryManager getFavoriteQueryDao(String project) {
        return FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        PostAddCuboidEvent event = (PostAddCuboidEvent) eventContext.getEvent();
        try {
            String project = eventContext.getProject();
            List<String> sqlList = event.getSqlPatterns();
            val jobId = event.getJobId();
            Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
            if (!checkSubjectExists(project, event.getModelId(), null, event)) {
                finishEvent(project, event.getId());
                return;
            }
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val merger = new AfterBuildResourceMerger(kylinConfig, project, JobTypeEnum.INDEX_BUILD);
            executable.getTasks().stream()
                    .filter(task -> task instanceof  NSparkExecutable)
                    .filter(task -> ((NSparkExecutable)task).needMergeMetadata())
                    .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));
            handleFavoriteQuery(project, sqlList);
            finishEvent(project, event.getId());
        } catch (Throwable throwable) {
            logger.error("Process event " + event.toString() + " failed:", throwable);
            throw throwable;
        }
    }

    @Override
    protected void restartNewJobIfNecessary(EventContext eventContext, ChainedExecutable executable) {
        val project = eventContext.getProject();
        val postEvent = (PostAddCuboidEvent) eventContext.getEvent();
        val job = (DefaultChainedExecutable) executable;
        // anyTargetSegmentExists && checkCuttingInJobByModel need restart job
        if (!(job.checkCuttingInJobByModel() && job.checkAnyTargetSegmentExists())) {
            return;
        }
        val addEvent = new AddCuboidEvent();
        addEvent.setModelId(postEvent.getModelId());
        addEvent.setOwner(postEvent.getOwner());
        addEvent.setJobId(UUID.randomUUID().toString());
        addEvent.setSqlPatterns(postEvent.getSqlPatterns());
        getEventManager(project, KylinConfig.getInstanceFromEnv()).post(addEvent);

        val postAddEvent = new PostAddCuboidEvent();
        postAddEvent.setModelId(addEvent.getModelId());
        postAddEvent.setJobId(addEvent.getJobId());
        postAddEvent.setOwner(addEvent.getOwner());
        postAddEvent.setSqlPatterns(addEvent.getSqlPatterns());
        getEventManager(project, KylinConfig.getInstanceFromEnv()).post(postAddEvent);
    }

    protected void doHandleWithNullJob(EventContext eventContext) {

        PostAddCuboidEvent event = (PostAddCuboidEvent) eventContext.getEvent();
        String project = eventContext.getProject();
        List<String> sqlList = event.getSqlPatterns();

        if (!checkSubjectExists(project, event.getModelId(), null, event)) {
            finishEvent(project, event.getId());
            return;
        }

        handleFavoriteQuery(project, sqlList);

        finishEvent(project, event.getId());
    }

    private void handleFavoriteQuery(String project, List<String> sqlList) {
        if (CollectionUtils.isNotEmpty(sqlList)) {
            for (String sqlPattern : sqlList) {
                getFavoriteQueryDao(project).updateStatus(sqlPattern, FavoriteQueryStatusEnum.FULLY_ACCELERATED, null);
            }
        }

    }

}