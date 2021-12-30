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
package io.kyligence.kap.rest.service.task;

import static io.kyligence.kap.metadata.epoch.EpochManager.GLOBAL;
import static io.kyligence.kap.metadata.favorite.AsyncTaskManager.ASYNC_ACCELERATION_TASK;
import static io.kyligence.kap.metadata.favorite.AsyncTaskManager.getInstance;
import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;

import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.AsyncAccelerationTask;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.RawRecService;
import lombok.extern.slf4j.Slf4j;

@Component("recommendationUpdateScheduler")
@Slf4j
public class RecommendationTopNUpdateScheduler {

    @Autowired
    private RawRecService rawRecService;

    private ScheduledThreadPoolExecutor taskScheduler;

    private Map<String, Future> needUpdateProjects = Maps.newConcurrentMap();

    public RecommendationTopNUpdateScheduler() {
        taskScheduler = new ScheduledThreadPoolExecutor(10, new NamedThreadFactory("recommendation-update-topn"));
        taskScheduler.setKeepAliveTime(1, TimeUnit.MINUTES);
        taskScheduler.allowCoreThreadTimeOut(true);
    }

    public synchronized void reScheduleProject(String project) {
        removeProject(project);
        addProject(project);
    }

    public synchronized void addProject(String project) {
        if (!needUpdateProjects.containsKey(project)) {
            scheduleNextTask(project, true);
        }
    }

    public synchronized void removeProject(String project) {
        Future task = needUpdateProjects.get(project);
        if (task != null) {
            log.debug("cancel {} future task", project);
            task.cancel(false);
        }
        needUpdateProjects.remove(project);
    }

    private synchronized boolean scheduleNextTask(String project, boolean isFirstSchedule) {
        if (!isFirstSchedule && !needUpdateProjects.containsKey(project)) {
            return false;
        }
        if (notOwner(project)) {
            needUpdateProjects.remove(project);
            return false;
        }
        if(!isFirstSchedule){
            saveTaskTime(project);
        }
        long nextMilliSeconds = computeNextTaskTimeGap(project);
        needUpdateProjects.put(project,
                taskScheduler.schedule(() -> work(project), nextMilliSeconds, TimeUnit.MILLISECONDS));
        return true;
    }

    private void work(String project) {
        if (!scheduleNextTask(project, false)) {
            log.debug("{} task can't run, skip this time", project);
            return;
        }
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL);
        try (SetThreadName ignored = new SetThreadName("UpdateTopNRecommendationsWorker")) {
            log.info("Routine task to update {} cost and topN recommendations", project);
            rawRecService.updateCostsAndTopNCandidates(project);
            log.info("Updating {} cost and topN recommendations finished.", project);
        }

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL);
    }

    private boolean notOwner(String project) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        EpochManager epochMgr = EpochManager.getInstance();
        return !kylinConfig.isUTEnv() && !epochMgr.checkEpochOwner(project);
    }

    @VisibleForTesting
    protected long computeNextTaskTimeGap(String project) {
        long lastTaskTime = getLastTaskTime(project);
        long nextTaskTime = computeNextTaskTime(lastTaskTime, project);
        log.debug("project {} next task time is {}", project, nextTaskTime);
        return nextTaskTime - lastTaskTime;
    }

    private long getLastTaskTime(String project) {
        AsyncAccelerationTask task = (AsyncAccelerationTask) getInstance(KylinConfig.getInstanceFromEnv(), project)
                .get(ASYNC_ACCELERATION_TASK);
        return task.getLastUpdateTopNTime() == 0 ? System.currentTimeMillis() : task.getLastUpdateTopNTime();
    }

    private void saveTaskTime(String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) getInstance(
                    KylinConfig.getInstanceFromEnv(), project).get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.setLastUpdateTopNTime(System.currentTimeMillis());
            getInstance(KylinConfig.getInstanceFromEnv(), project).save(asyncAcceleration);
            return null;
        }, project);
    }

    private long computeNextTaskTime(long lastTaskTime, String project) {
        ProjectInstance projectInst = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
        long lastTaskDayStart = getDateInMillis(lastTaskTime);
        int days = Integer.parseInt(FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getValue(FavoriteRule.UPDATE_FREQUENCY));
        long taskStartInDay = LocalTime.parse(projectInst.getConfig().getUpdateTopNTime()).toSecondOfDay() * 1000L;
        long nextTaskTime = lastTaskDayStart + MILLIS_PER_DAY * days + taskStartInDay;
        return nextTaskTime > lastTaskTime ? nextTaskTime
                : lastTaskTime + projectInst.getConfig().getUpdateTopNTimeGap();
    }

    private long getDateInMillis(final long queryTime) {
        return TimeUtil.getDayStart(queryTime);
    }

}
