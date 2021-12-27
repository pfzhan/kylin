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

import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
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
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.RawRecService;
import lombok.extern.slf4j.Slf4j;

@Component("recommendationUpdateScheduler")
@Slf4j
public class RecommendationTopNUpdateScheduler {

    private static final long ONE_DAY_TO_MILLISECONDS = 24 * 60 * 60 * 1000;

    @Autowired
    private RawRecService rawRecService;

    private ScheduledThreadPoolExecutor taskScheduler;

    private Map<String, ScheduledFuture> needUpdateProjects = Maps.newConcurrentMap();

    public RecommendationTopNUpdateScheduler() {
        taskScheduler = new ScheduledThreadPoolExecutor(10, new NamedThreadFactory("recommendation-update-topn"));
        taskScheduler.setKeepAliveTime(5, TimeUnit.MINUTES);
        taskScheduler.allowCoreThreadTimeOut(true);
    }

    public synchronized void addProject(String project) {
        if (!needUpdateProjects.containsKey(project)) {
            scheduleNextTask(project, true);
        }
    }

    public synchronized void removeProject(String project) {
        ScheduledFuture task = needUpdateProjects.get(project);
        if (task != null) {
            log.debug("cancel future task");
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
        long nextMilliSeconds = computeNextTaskTimeGap(project);
        needUpdateProjects.put(project,
                taskScheduler.schedule(() -> work(project), nextMilliSeconds, TimeUnit.MILLISECONDS));
        return true;
    }

    private void work(String project) {
        if (!scheduleNextTask(project, false)) {
            log.debug("cancel this task");
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
        EpochManager epochMgr = EpochManager.getInstance(kylinConfig);
        return !kylinConfig.isUTEnv() && !epochMgr.checkEpochOwner(project);
    }

    @VisibleForTesting
    protected long computeNextTaskTimeGap(String project) {
        long currentTime = System.currentTimeMillis();
        long nextTaskTime = computeNextTaskTime(currentTime, project);
        log.debug("project {} next task time is {}", project, nextTaskTime);
        return nextTaskTime - currentTime;
    }

    private long computeNextTaskTime(long currentTime, String project) {
        ProjectInstance projectInst = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
        long lastTaskDayStart = getDateInMillis(currentTime);
        int days = Integer.parseInt(FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getValue(FavoriteRule.UPDATE_FREQUENCY));
        long taskStartInDay = LocalTime.parse(projectInst.getConfig().getUpdateTopNTime()).toSecondOfDay() * 1000;
        long nextTaskTime = lastTaskDayStart + ONE_DAY_TO_MILLISECONDS * days + taskStartInDay;
        return nextTaskTime > currentTime ? nextTaskTime : currentTime + projectInst.getConfig().getUpdateTopNTimeGap();
    }

    private long getDateInMillis(final long queryTime) {
        return TimeUtil.getDayStart(queryTime);
    }

}
