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
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.JobRelatedEvent;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEventPostJobHandler extends AbstractEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEventPostJobHandler.class);
    protected static final String SUBJECT_NOT_EXIST_COMMENT = "subject does not exist or is broken, roll back to to-be-accelerated status";

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
        } else if (executable.getStatus() == ExecutableState.SUICIDAL
                || executable.getStatus() == ExecutableState.DISCARDED) {
            doHandleWithSuicidalJob(eventContext, executable);
            log.debug("previous job suicide, current event:{} will be ignored", eventContext.getEvent());
            handleFavoriteQuery(eventContext);
            finishEvent(eventContext.getProject(), eventContext.getEvent().getId());
            return;
        }
        doHandle(eventContext, executable);
    }

    protected void doHandleWithSuicidalJob(EventContext eventContext, ChainedExecutable executable) {
    }

    protected abstract void doHandleWithNullJob(EventContext eventContext);

    /**
     *
     * @param eventContext
     * @param executable
     */
    protected abstract void doHandle(EventContext eventContext, ChainedExecutable executable);

    protected void rollFQBackToInitialStatus(EventContext eventContext, String comment) {
        val project = eventContext.getProject();
        val model = eventContext.getEvent().getModelId();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val fqManager = FavoriteQueryManager.getInstance(kylinConfig, project);

        for (val fq : fqManager.getAll()) {
            if (!fq.getStatus().equals(FavoriteQueryStatusEnum.ACCELERATING))
                continue;

            if (fq.getRealizations().stream().anyMatch(fqr -> fqr.getModelId().equals(model)))
                fqManager.rollBackToInitialStatus(fq.getSqlPattern(), comment);
        }
    }

    protected void handleFavoriteQuery(EventContext eventContext) {
        val project = eventContext.getProject();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val fqManager = FavoriteQueryManager.getInstance(kylinConfig, project);

        for (val fq : fqManager.getAll()) {
            int finishedCount = 0;

            for (val fqr : fq.getRealizations()) {
                String fqrModelId = fqr.getModelId();
                long layoutId = fqr.getLayoutId();
                val df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(fqrModelId);
                if (df == null || df.checkBrokenWithRelatedInfo()) {
                    logger.info("Model {} is broken or deleted", fqrModelId);
                    fqManager.rollBackToInitialStatus(fq.getSqlPattern(), SUBJECT_NOT_EXIST_COMMENT);
                    break;
                }

                val readySegs = df.getSegments(SegmentStatusEnum.READY);
                if (readySegs.isEmpty()) {
                    logger.info("no ready segment exists in target index plan {}", fqrModelId);
                    break;
                }

                if (df.getIndexPlan().getCuboidLayout(layoutId) == null) {
                    logger.info(
                            "Layout {} does not exist in target index plan {}, gonna put Favorite Query {} status back to TO-BE-ACCELERATED",
                            layoutId, fqrModelId, fq.getId());
                    fqManager.rollBackToInitialStatus(fq.getSqlPattern(), "Layout does not exist");
                    break;
                }

                val lastReadySeg = readySegs.getLatestReadySegment();
                val dataLayout = lastReadySeg.getLayout(layoutId);

                finishedCount += dataLayout == null ? 0 : 1;
            }

            if (finishedCount > 0 && finishedCount == fq.getRealizations().size()
                    && fq.getStatus() != FavoriteQueryStatusEnum.ACCELERATED)
                fqManager.updateStatus(fq.getSqlPattern(), FavoriteQueryStatusEnum.ACCELERATED, null);
        }
    }
}
