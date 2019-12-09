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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.engine.spark.job.NSparkExecutable;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class PostAddCuboidHandler extends AbstractEventPostJobHandler {
    private static final Logger logger = LoggerFactory.getLogger(PostAddCuboidHandler.class);

    @Override
    protected void doHandle(EventContext eventContext, ChainedExecutable executable) {
        PostAddCuboidEvent event = (PostAddCuboidEvent) eventContext.getEvent();
        try {
            String project = eventContext.getProject();
            val jobId = event.getJobId();
            Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
            if (!checkSubjectExists(project, event.getModelId(), null, event)) {
                rollFQBackToInitialStatus(eventContext, SUBJECT_NOT_EXIST_COMMENT);
                finishEvent(project, event.getId());
                return;
            }
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val merger = new AfterBuildResourceMerger(kylinConfig, project);
            executable.getTasks().stream() //
                    .filter(task -> task instanceof NSparkExecutable) //
                    .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                    .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));

            Optional.ofNullable(executable.getParams()).ifPresent(params -> {
                String toBeDeletedLayoutIdsStr = params.get(NBatchConstants.P_TO_BE_DELETED_LAYOUT_IDS);
                if (StringUtils.isNotBlank(toBeDeletedLayoutIdsStr)) {
                    logger.info("Try to delete the toBeDeletedLayoutIdsStr: {}, jobId: {}", toBeDeletedLayoutIdsStr,
                            jobId);
                    Set<Long> toBeDeletedLayoutIds = new LinkedHashSet<>();
                    for (String id : toBeDeletedLayoutIdsStr.split(",")) {
                        toBeDeletedLayoutIds.add(Long.parseLong(id));
                    }

                    NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                            .updateIndexPlan(event.getModelId(), copyForWrite -> {
                                copyForWrite.removeLayoutsFromToBeDeletedList(toBeDeletedLayoutIds,
                                        LayoutEntity::equals, true, true);
                            });
                }
            });

            handleFavoriteQuery(eventContext);
            finishEvent(project, event.getId());
        } catch (Throwable throwable) {
            logger.error("Process event " + event.toString() + " failed:", throwable);
            throw throwable;
        }
    }

    @Override
    protected void doHandleWithSuicidalJob(EventContext eventContext, ChainedExecutable executable) {
        val project = eventContext.getProject();
        val postEvent = (PostAddCuboidEvent) eventContext.getEvent();
        val job = (DefaultChainedExecutableOnModel) executable;
        // anyTargetSegmentExists && checkCuttingInJobByModel need restart job
        if (!(job.checkCuttingInJobByModel() && job.checkAnyTargetSegmentExists())) {
            return;
        }
        getEventManager(project, KylinConfig.getInstanceFromEnv()).postAddCuboidEvents(postEvent.getModelId(),
                postEvent.getOwner());
    }

    protected void doHandleWithNullJob(EventContext eventContext) {

        PostAddCuboidEvent event = (PostAddCuboidEvent) eventContext.getEvent();
        String project = eventContext.getProject();

        if (!checkSubjectExists(project, event.getModelId(), null, event)) {
            rollFQBackToInitialStatus(eventContext, SUBJECT_NOT_EXIST_COMMENT);
            finishEvent(project, event.getId());
            return;
        }

        handleFavoriteQuery(eventContext);
        finishEvent(project, event.getId());
    }
}