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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.rest.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffsetManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import lombok.Getter;
import lombok.Setter;

public class NFavoriteScheduler {
    private static final Logger logger = LoggerFactory.getLogger(NFavoriteScheduler.class);

    private ScheduledExecutorService autoFavoriteScheduler;
    private ScheduledExecutorService updateFavoriteScheduler;

    @Getter
    private String project;

    private TreeSet<FrequencyStatus> frequencyStatuses = new TreeSet<>();
    private FrequencyStatus overAllStatus = new FrequencyStatus();

    // 24 hrs time window
    private static long frequencyTimeWindow = 24 * 60 * 60 * 1000L;
    // handles the case when the actual time of inserting to influx database is later than recorded time
    private int backwardShiftTime;
    private boolean hasStarted;

    private static final Map<String, NFavoriteScheduler> INSTANCE_MAP = Maps.newConcurrentMap();

    public NFavoriteScheduler(String project) {
        Preconditions.checkNotNull(project);

        this.project = project;
        backwardShiftTime = KapConfig.getInstanceFromEnv().getInfluxDBFlushDuration() * 2;

        logger.debug("New NFavoriteScheduler created by project {}", project);
    }

    public static NFavoriteScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, NFavoriteScheduler::new);
    }

    public void init() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);

        // init schedulers
        autoFavoriteScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("AutoFavoriteWorker(project:" + project + ")"));
        updateFavoriteScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("UpdateFQWorker(project:" + project + ")"));

        // init frequency status
        autoFavoriteScheduler.schedule(this::initFrequencyStatus, 0, TimeUnit.SECONDS);

        // schedule runner at fixed interval
        int initialDelay = new Random().nextInt(projectInstance.getConfig().getAutoMarkFavoriteInterval());
        autoFavoriteScheduler.scheduleAtFixedRate(new AutoFavoriteRunner(), initialDelay,
                projectInstance.getConfig().getAutoMarkFavoriteInterval(), TimeUnit.SECONDS);
        updateFavoriteScheduler.scheduleAtFixedRate(new UpdateFavoriteStatisticsRunner(), initialDelay + 10L,
                projectInstance.getConfig().getFavoriteStatisticsCollectionInterval(), TimeUnit.SECONDS);

        hasStarted = true;
        logger.info("Auto favorite scheduler is started for [{}] ", project);
    }

    void initFrequencyStatus() {
        adjustTimeOffset();

        QueryHistoryTimeOffset queryHistoryTimeOffset = QueryHistoryTimeOffsetManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
        long fetchQueryHistoryGapTime = getFetchQueryHistoryGapTime();
        long lastAutoMarkTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();

        // init frequency status in past 24 hrs
        long startTime = lastAutoMarkTime - frequencyTimeWindow;
        long endTime = startTime + fetchQueryHistoryGapTime;

        // past 24hr
        while (endTime <= lastAutoMarkTime) {
            List<QueryHistory> queryHistories = getQueryHistoryDao().getQueryHistoriesByTime(startTime, endTime);

            FrequencyStatus frequencyStatus = new FrequencyStatus(startTime);

            for (QueryHistory queryHistory : queryHistories) {
                if (!isQualifiedCandidate(queryHistory))
                    continue;

                frequencyStatus.updateFrequency(queryHistory.getSqlPattern());
            }

            updateOverallFrequencyStatus(frequencyStatus, frequencyStatuses, overAllStatus);

            startTime = endTime;
            endTime += fetchQueryHistoryGapTime;
        }
    }

    private long getFetchQueryHistoryGapTime() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        return projectInstance.getConfig().getQueryHistoryScanPeriod();
    }

    void adjustTimeOffset() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            QueryHistoryTimeOffsetManager timeOffsetManager = QueryHistoryTimeOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project);
            QueryHistoryTimeOffset queryHistoryTimeOffset = timeOffsetManager.get();
            ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject(project);
            long maxIntervalInMillis = projectInstance.getConfig().getQueryHistoryMaxScanInterval();

            long lastAutoMarkTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();
            long lastUpdateTime = queryHistoryTimeOffset.getFavoriteQueryUpdateTimeOffset();
            long currentTime = getSystemTime();

            // move time offset to the most recent month
            if (currentTime - lastAutoMarkTime > maxIntervalInMillis)
                lastAutoMarkTime = currentTime - maxIntervalInMillis;

            if (currentTime - lastUpdateTime > maxIntervalInMillis)
                lastUpdateTime = currentTime - maxIntervalInMillis;

            queryHistoryTimeOffset.setAutoMarkTimeOffset(lastAutoMarkTime);
            queryHistoryTimeOffset.setFavoriteQueryUpdateTimeOffset(lastUpdateTime);

            timeOffsetManager.save(queryHistoryTimeOffset);

            return 0;
        }, project);
    }

    QueryHistoryDAO getQueryHistoryDao() {
        return QueryHistoryDAO.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    private TreeSet<FrequencyStatus> deepCopyFrequencyStatues() {
        TreeSet<FrequencyStatus> copied = new TreeSet<>();
        frequencyStatuses.forEach(input -> copied.add(copyFrequencyStatus(input)));

        return copied;
    }

    private FrequencyStatus copyFrequencyStatus(FrequencyStatus status) {
        return new FrequencyStatus(Maps.newHashMap(status.getSqlPatternFreqMap()), status.getTime());
    }

    public class AutoFavoriteRunner implements Runnable {
        private TreeSet<FrequencyStatus> copiedFrequencyStatues;
        private FrequencyStatus copiedOverallStatus;

        @Override
        public void run() {
            copiedFrequencyStatues = deepCopyFrequencyStatues();
            copiedOverallStatus = copyFrequencyStatus(overAllStatus);

            try {
                autoFavorite();
            } catch (Exception e) {
                logger.error("Error {} caught when doing auto favorite for project {} ", e.getMessage(), project);
                return;
            }

            frequencyStatuses = copiedFrequencyStatues;
            overAllStatus = copiedOverallStatus;

            copiedFrequencyStatues = null;
            copiedOverallStatus = null;
        }

        private void autoFavorite() {
            Set<FavoriteQuery> candidates = new HashSet<>();

            // scan query history
            AutoFavoriteInfo autoFavoriteInfo = scanQueryHistoryByTime(candidates);

            // filter by frequency rule
            addCandidatesByFrequencyRule(candidates);

            // update related metadata
            UnitOfWork.doInTransactionWithRetry(() -> {
                internalFavorite(candidates);

                KylinConfig config = KylinConfig.getInstanceFromEnv();
                QueryHistoryTimeOffsetManager timeOffsetManager = QueryHistoryTimeOffsetManager.getInstance(config,
                        project);
                AccelerateRatioManager accelerateRatioManager = AccelerateRatioManager.getInstance(config, project);

                // update time offset
                QueryHistoryTimeOffset timeOffset = timeOffsetManager.get();
                timeOffset.setAutoMarkTimeOffset(autoFavoriteInfo.getScannedTimeOffset());
                timeOffsetManager.save(timeOffset);
                // update accelerate ratio
                accelerateRatioManager.increment(autoFavoriteInfo.getQueryMarkedAsFavoriteNum(),
                        autoFavoriteInfo.getOverallQueryNum());
                return 0;
            }, project);
        }

        private AutoFavoriteInfo scanQueryHistoryByTime(Set<FavoriteQuery> candidates) {
            QueryHistoryTimeOffset queryHistoryTimeOffset = QueryHistoryTimeOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).get();

            long fetchQueryHistoryGapTime = getFetchQueryHistoryGapTime();
            long startTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();
            long endTime = startTime + fetchQueryHistoryGapTime;
            long maxTime = getSystemTime() - backwardShiftTime;

            int queryMarkedAsFavoriteNum = 0;
            int overallQueryNum = 0;

            while (endTime <= maxTime) {
                List<QueryHistory> queryHistories = getQueryHistoryDao().getQueryHistoriesByTime(startTime, endTime);

                FrequencyStatus newStatus = new FrequencyStatus(startTime);

                for (QueryHistory queryHistory : queryHistories) {
                    // failed query
                    if (queryHistory.isException())
                        continue;

                    overallQueryNum++;
                    if (!isQualifiedCandidate(queryHistory))
                        continue;

                    String sqlPattern = queryHistory.getSqlPattern();
                    newStatus.updateFrequency(sqlPattern);

                    if (FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                            .contains(sqlPattern)) {
                        queryMarkedAsFavoriteNum++;
                        continue;
                    }

                    if (matchRuleBySingleRecord(queryHistory)) {
                        final FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern);
                        favoriteQuery.setLastQueryTime(queryHistory.getQueryTime());
                        favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
                        candidates.add(favoriteQuery);
                    }
                }

                updateOverallFrequencyStatus(newStatus, copiedFrequencyStatues, copiedOverallStatus);

                startTime = endTime;
                endTime += fetchQueryHistoryGapTime;
            }

            return new AutoFavoriteInfo(startTime, queryMarkedAsFavoriteNum, overallQueryNum);
        }

        private void internalFavorite(final Set<FavoriteQuery> favoriteQueries) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            FavoriteQueryManager manager = FavoriteQueryManager.getInstance(config, project);
            manager.create(favoriteQueries);

            ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(project);
            if ((projectInstance.getConfig().getFavoriteQueryAccelerateThresholdBatchEnabled())
                    && projectInstance.getConfig().getFavoriteQueryAccelerateThresholdAutoApply()) {
                List<String> unAcceeleratedSqlPattern = manager.getUnAcceleratedSqlPattern();
                if (unAcceeleratedSqlPattern.size() < projectInstance.getConfig()
                        .getFavoriteQueryAccelerateThreshold()) {
                    return;
                }
                // accelerate
                FavoriteQueryService.accelerate(unAcceeleratedSqlPattern, project, config);
            }
        }

        private void addCandidatesByFrequencyRule(Set<FavoriteQuery> candidates) {
            FavoriteRule freqRule = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getByName(FavoriteRule.FREQUENCY_RULE_NAME);
            Preconditions.checkArgument(freqRule != null);

            if (!freqRule.isEnabled())
                return;
            Map<Integer, Set<String>> distinctFreqMap = Maps.newHashMap();

            for (Map.Entry<String, Integer> entry : copiedOverallStatus.getSqlPatternFreqMap().entrySet()) {
                String sqlPattern = entry.getKey();
                int frequency = entry.getValue();

                Set<String> sqlPatternSet = distinctFreqMap.get(frequency);
                if (sqlPatternSet == null)
                    sqlPatternSet = new HashSet<>();

                sqlPatternSet.add(sqlPattern);
                distinctFreqMap.put(frequency, sqlPatternSet);
            }

            FavoriteRule.Condition condition = (FavoriteRule.Condition) freqRule.getConds().get(0);

            int topK = (int) Math.floor(distinctFreqMap.size() * Float.valueOf(condition.getRightThreshold()));
            addCandidates(candidates, distinctFreqMap, topK);
        }

        private void addCandidates(Set<FavoriteQuery> candidates, Map<Integer, Set<String>> sqlPatternsMap, int topK) {
            if (topK < 1)
                return;

            List<Integer> orderingResult = Ordering.natural().greatestOf(sqlPatternsMap.keySet(), topK);

            for (int frequency : orderingResult) {
                for (String sqlPattern : sqlPatternsMap.get(frequency)) {
                    if (FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project).contains(sqlPattern))
                        continue;
                    FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern);
                    favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
                    candidates.add(favoriteQuery);
                }
            }
        }
    }

    @Getter
    @AllArgsConstructor
    private class AutoFavoriteInfo {
        private long scannedTimeOffset;
        private int queryMarkedAsFavoriteNum;
        private int overallQueryNum;
    }

    private boolean isQualifiedCandidate(QueryHistory queryHistory) {
        String sqlPattern = queryHistory.getSqlPattern();
        if (isInBlacklist(sqlPattern, project))
            return false;

        // query with constants, 1 <> 1
        if (queryHistory.getAnsweredBy().contains("CONSTANTS"))
            return false;

        return true;
    }

    long getSystemTime() {
        return System.currentTimeMillis();
    }

    public FrequencyStatus getOverAllStatus() {
        return overAllStatus;
    }

    public TreeSet<FrequencyStatus> getFrequencyStatuses() {
        return frequencyStatuses;
    }

    private void updateOverallFrequencyStatus(FrequencyStatus newStatus, TreeSet<FrequencyStatus> frequencyStatuses,
            FrequencyStatus overAllStatus) {
        frequencyStatuses.add(newStatus);

        // remove status beyond 24hrs
        while (newStatus.getTime() - frequencyStatuses.first().getTime() >= frequencyTimeWindow) {
            FrequencyStatus removedStatus = frequencyStatuses.pollFirst();
            overAllStatus.removeStatus(removedStatus);
        }

        // add status
        overAllStatus.addStatus(newStatus);
    }

    boolean matchRuleBySingleRecord(QueryHistory queryHistory) {
        List<FavoriteRule> rules = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getAllEnabled();

        for (FavoriteRule rule : rules) {
            if (rule.getName().equals(FavoriteRule.SUBMITTER_RULE_NAME)) {
                for (FavoriteRule.AbstractCondition submitterCond : rule.getConds()) {
                    if (queryHistory.getQuerySubmitter()
                            .equals(((FavoriteRule.Condition) submitterCond).getRightThreshold()))
                        return true;
                }
            }

            if (rule.getName().equals(FavoriteRule.DURATION_RULE_NAME)) {
                FavoriteRule.Condition durationCond = (FavoriteRule.Condition) rule.getConds().get(0);
                if (queryHistory.getDuration() >= Long.valueOf(durationCond.getLeftThreshold()) * 1000L
                        && queryHistory.getDuration() <= Long.valueOf(durationCond.getRightThreshold()) * 1000L)
                    return true;
            }
        }

        return false;
    }

    private boolean isInBlacklist(String sqlPattern, String project) {
        FavoriteRule blacklist = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getByName(FavoriteRule.BLACKLIST_NAME);
        List<FavoriteRule.AbstractCondition> conditions = blacklist.getConds();

        for (FavoriteRule.AbstractCondition condition : conditions) {
            if (sqlPattern.equalsIgnoreCase(((FavoriteRule.SQLCondition) condition).getSqlPattern()))
                return true;
        }

        return false;
    }

    public void scheduleAutoFavorite() {
        autoFavoriteScheduler.schedule(new AutoFavoriteRunner(), 0, TimeUnit.SECONDS);
    }

    public boolean hasStarted() {
        return this.hasStarted;
    }

    public class UpdateFavoriteStatisticsRunner implements Runnable {

        @Override
        public void run() {
            try {
                updateFavoriteStatistics();
            } catch (Exception ex) {
                logger.error("Error {} caught when updating favorite queries for project {}", ex.getMessage(), project);
            }
        }

        private void updateFavoriteStatistics() {
            QueryHistoryTimeOffset timeOffset = QueryHistoryTimeOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
            long endTime = getSystemTime() - backwardShiftTime;
            List<QueryHistory> queryHistories = getQueryHistoryDao()
                    .getQueryHistoriesByTime(timeOffset.getFavoriteQueryUpdateTimeOffset(), endTime);

            updateRelatedMetadata(queryHistories, endTime);
        }

        private void updateRelatedMetadata(List<QueryHistory> queryHistories, long scannedOffset) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Map<String, FavoriteQuery> favoritesAboutToUpdate = Maps.newHashMap();

            Map<String, Integer> dfHitCountMap = Maps.newHashMap();
            for (QueryHistory queryHistory : queryHistories) {
                if (StringUtils.isNotEmpty(queryHistory.getAnsweredBy())) {
                    val answers = Lists.newArrayList(queryHistory.getAnsweredBy().split(","));
                    for (val answer : answers) {
                        if (dfManager.getDataflow(answer) != null) {
                            dfHitCountMap.merge(answer, 1, Integer::sum);
                        }
                    }
                }
                updateFavoriteQuery(queryHistory, favoritesAboutToUpdate);
            }

            List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
            for (Map.Entry<String, FavoriteQuery> favoritesInProj : favoritesAboutToUpdate.entrySet()) {
                favoriteQueries.add(favoritesInProj.getValue());
            }

            UnitOfWork.doInTransactionWithRetry(() -> {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                // update model usage
                incQueryHitCount(dfHitCountMap);
                // update favorite query statistics
                FavoriteQueryManager.getInstance(config, project).updateStatistics(favoriteQueries);
                QueryHistoryTimeOffset timeOffset = QueryHistoryTimeOffsetManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
                timeOffset.setFavoriteQueryUpdateTimeOffset(scannedOffset);
                QueryHistoryTimeOffsetManager.getInstance(config, project).save(timeOffset);
                return 0;
            }, project);
        }

        private void incQueryHitCount(Map<String, Integer> dfHitCountMap) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for (val entry : dfHitCountMap.entrySet()) {
                if (dfManager.getDataflow(entry.getKey()) != null) {
                    dfManager.updateDataflow(entry.getKey(), copyForWrite -> copyForWrite
                            .setQueryHitCount(copyForWrite.getQueryHitCount() + entry.getValue()));
                }
            }
        }

        private void updateFavoriteQuery(QueryHistory queryHistory, Map<String, FavoriteQuery> favoritesAboutToUpdate) {
            String sqlPattern = queryHistory.getSqlPattern();

            if (!FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project).contains(sqlPattern))
                return;

            FavoriteQuery favoriteQuery = favoritesAboutToUpdate.get(sqlPattern);
            if (favoriteQuery == null) {
                favoriteQuery = new FavoriteQuery(sqlPattern);
            }

            favoriteQuery.incStats(queryHistory);
            favoritesAboutToUpdate.put(sqlPattern, favoriteQuery);
        }
    }

    public static synchronized void shutdownByProject(String project) {
        val instance = getInstanceByProject(project);
        if (instance != null) {
            instance.shutdown();
            INSTANCE_MAP.remove(project);
        }
    }

    public static synchronized NFavoriteScheduler getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    private void shutdown() {
        logger.info("Shutting down DefaultScheduler ....");
        if (autoFavoriteScheduler != null) {
            ExecutorServiceUtil.forceShutdown(autoFavoriteScheduler);
        }
        if (updateFavoriteScheduler != null) {
            ExecutorServiceUtil.forceShutdown(updateFavoriteScheduler);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public class FrequencyStatus implements Comparable<FrequencyStatus> {
        // key is the sql pattern, value is the frequency
        private Map<String, Integer> sqlPatternFreqMap = Maps.newHashMap();
        private long time;

        public FrequencyStatus(long time) {
            this.time = time;
        }

        public void updateFrequency(final String sqlPattern) {
            Integer frequency = sqlPatternFreqMap.get(sqlPattern);
            if (frequency == null)
                frequency = 1;
            else
                frequency++;
            sqlPatternFreqMap.put(sqlPattern, frequency);
        }

        public void addStatus(final FrequencyStatus newStatus) {
            for (Map.Entry<String, Integer> newFreqMap : newStatus.getSqlPatternFreqMap().entrySet()) {
                String sqlPattern = newFreqMap.getKey();
                Integer currentFreq = this.sqlPatternFreqMap.get(sqlPattern);
                if (currentFreq == null)
                    currentFreq = newFreqMap.getValue();
                else
                    currentFreq += newFreqMap.getValue();

                this.sqlPatternFreqMap.put(sqlPattern, currentFreq);
            }
        }

        public void removeStatus(final FrequencyStatus removedStatus) {
            for (Map.Entry<String, Integer> removedFreqMap : removedStatus.getSqlPatternFreqMap().entrySet()) {
                String sqlPattern = removedFreqMap.getKey();
                Integer currentFreq = this.sqlPatternFreqMap.get(sqlPattern);

                if (currentFreq != null) {
                    currentFreq -= removedFreqMap.getValue();
                    if (currentFreq <= 0) {
                        this.sqlPatternFreqMap.remove(sqlPattern);
                        continue;
                    }
                }

                this.sqlPatternFreqMap.put(sqlPattern, currentFreq);
            }
        }

        @Override
        public int compareTo(FrequencyStatus o) {
            if (this.time == o.getTime())
                return 0;

            return this.time > o.getTime() ? 1 : -1;
        }
    }
}
