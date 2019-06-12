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

import com.google.common.collect.Sets;
import io.kyligence.kap.rest.service.task.QueryHistoryAccessor;
import io.kyligence.kap.rest.service.task.UpdateUsageStatisticsRunner;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.security.KylinUserManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
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
    private QueryHistoryAccessor queryHistoryAccessor;

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
        this.queryHistoryAccessor = new QueryHistoryAccessor(project);
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
                new NamedThreadFactory("UpdateUsageWorker(project:" + project + ")"));

        // adjust time offset
        adjustTimeOffset();

        int initialDelay = new Random().nextInt(60);
        // init frequency status
        autoFavoriteScheduler.schedule(this::initFrequencyStatus, initialDelay, TimeUnit.SECONDS);

        // auto favorite and update favorite interval times should be at least 60s
        long autoFavoriteIntervalTime = projectInstance.getConfig().getAutoMarkFavoriteInterval();
        long updateFavoriteIntervalTime = projectInstance.getConfig().getFavoriteStatisticsCollectionInterval();
        Preconditions.checkArgument(autoFavoriteIntervalTime * 1000L >= queryHistoryAccessor.getFetchQueryHistoryGapTime());
        Preconditions.checkArgument(updateFavoriteIntervalTime * 1000L >= queryHistoryAccessor.getFetchQueryHistoryGapTime());

        // schedule runner at fixed interval
        autoFavoriteScheduler.scheduleWithFixedDelay(new AutoFavoriteRunner(), initialDelay, autoFavoriteIntervalTime,
                TimeUnit.SECONDS);
        updateFavoriteScheduler.scheduleWithFixedDelay(new UpdateUsageStatisticsRunner(project), initialDelay + 10L,
                updateFavoriteIntervalTime, TimeUnit.SECONDS);

        hasStarted = true;
        logger.info("Auto favorite scheduler is started for [{}] ", project);
    }

    void initFrequencyStatus() {
        QueryHistoryTimeOffset queryHistoryTimeOffset = QueryHistoryTimeOffsetManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
        long fetchQueryHistoryGapTime = queryHistoryAccessor.getFetchQueryHistoryGapTime();
        long lastAutoMarkTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();

        // init frequency status in past 24 hrs
        long startTime = lastAutoMarkTime - frequencyTimeWindow;
        long endTime = startTime + fetchQueryHistoryGapTime;

        // past 24hr
        while (endTime <= lastAutoMarkTime) {
            List<QueryHistory> queryHistories = getQueryHistoryDao().getQueryHistoriesByTime(startTime, endTime);

            if (CollectionUtils.isEmpty(queryHistories)) {
                long firstQHTime = queryHistoryAccessor.skipEmptyIntervals(endTime, lastAutoMarkTime);
                startTime = firstQHTime - (firstQHTime - endTime) % fetchQueryHistoryGapTime;
                endTime = startTime + fetchQueryHistoryGapTime;
                continue;
            }

            FrequencyStatus frequencyStatus = new FrequencyStatus(startTime);

            for (QueryHistory queryHistory : queryHistories) {
                if (!isQualifiedCandidate(queryHistory))
                    continue;

                // get string reference from OverallStatus to avoid a big waste of String object
                String sqlPattern = overAllStatus.getSqlPatterns().getOrDefault(queryHistory.getSqlPattern(),
                        queryHistory.getSqlPattern());
                frequencyStatus.updateFrequency(sqlPattern);
            }

            updateOverallFrequencyStatus(frequencyStatus, frequencyStatuses, overAllStatus);

            startTime = endTime;
            endTime += fetchQueryHistoryGapTime;
        }
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
            long currentTime = queryHistoryAccessor.getSystemTime();

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

    public QueryHistoryDAO getQueryHistoryDao() {
        return queryHistoryAccessor.getQueryHistoryDao();
    }

    private TreeSet<FrequencyStatus> deepCopyFrequencyStatues() {
        TreeSet<FrequencyStatus> copied = new TreeSet<>();
        frequencyStatuses.forEach(input -> copied.add(copyFrequencyStatus(input)));

        return copied;
    }

    private FrequencyStatus copyFrequencyStatus(FrequencyStatus status) {
        return new FrequencyStatus(Maps.newHashMap(status.getSqlPatternFreqMap()),
                Maps.newHashMap(status.getSqlPatterns()), status.getTime());
    }

    public class AutoFavoriteRunner implements Runnable {
        private TreeSet<FrequencyStatus> copiedFrequencyStatues;
        private FrequencyStatus copiedOverallStatus;

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
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
            logger.info("auto favorite runner takes {}ms", System.currentTimeMillis() - startTime);
        }

        private void autoFavorite() {
            // scan query history
            scanQueryHistoryByTime();

            // filter by frequency rule
            Set<FavoriteQuery> candidates = getCandidatesByFrequencyRule();

            if (CollectionUtils.isNotEmpty(candidates)) {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    KylinConfig config = KylinConfig.getInstanceFromEnv();
                    FavoriteQueryManager manager = FavoriteQueryManager.getInstance(config, project);
                    manager.create(candidates);
                    return 0;
                }, project);
            }
        }

        private void updateRelatedMetadata(Set<FavoriteQuery> candidates, long autoFavoriteTimeOffset,
                                           int numOfQueryHitIndex, int overallQueryNum) {
            // update related metadata
            UnitOfWork.doInTransactionWithRetry(() -> {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                FavoriteQueryManager manager = FavoriteQueryManager.getInstance(config, project);
                manager.create(candidates);

                QueryHistoryTimeOffsetManager timeOffsetManager = QueryHistoryTimeOffsetManager.getInstance(config,
                        project);

                // update time offset
                QueryHistoryTimeOffset timeOffset = timeOffsetManager.get();
                timeOffset.setAutoMarkTimeOffset(autoFavoriteTimeOffset);
                timeOffsetManager.save(timeOffset);

                // update accelerate ratio
                if (numOfQueryHitIndex != 0 || overallQueryNum != 0) {
                    AccelerateRatioManager accelerateRatioManager = AccelerateRatioManager.getInstance(config, project);
                    accelerateRatioManager.increment(numOfQueryHitIndex, overallQueryNum);
                }
                return 0;
            }, project);
        }

        private void scanQueryHistoryByTime() {
            QueryHistoryTimeOffset queryHistoryTimeOffset = QueryHistoryTimeOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).get();

            long fetchQueryHistoryGapTime = queryHistoryAccessor.getFetchQueryHistoryGapTime();
            long startTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();
            long endTime = startTime + fetchQueryHistoryGapTime;
            long maxTime = queryHistoryAccessor.getSystemTime() - backwardShiftTime;

            List<QueryHistory> queryHistories;

            while (endTime <= maxTime) {
                queryHistories = getQueryHistoryDao().getQueryHistoriesByTime(startTime, endTime);

                if (CollectionUtils.isEmpty(queryHistories)) {
                    long firstQHTime = queryHistoryAccessor.skipEmptyIntervals(endTime, maxTime);
                    startTime = firstQHTime - (firstQHTime - endTime) % fetchQueryHistoryGapTime;
                    endTime = startTime + fetchQueryHistoryGapTime;
                    updateRelatedMetadata(Sets.newHashSet(), startTime, 0, 0);
                    continue;
                }

                findAllCandidates(queryHistories, startTime, endTime);

                startTime = endTime;
                endTime += fetchQueryHistoryGapTime;
            }

            if (startTime < maxTime) {
                queryHistories = getQueryHistoryDao().getQueryHistoriesByTime(startTime, maxTime);
                findAllCandidates(queryHistories, startTime, maxTime);
            }
        }

        private void findAllCandidates(List<QueryHistory> queryHistories, long startTime, long endTime) {
            int numOfQueryHitIndex = 0;
            int overallQueryNum = 0;

            FrequencyStatus newStatus = new FrequencyStatus(startTime);
            Set<FavoriteQuery> candidates = Sets.newHashSet();

            for (QueryHistory queryHistory : queryHistories) {
                overallQueryNum++;
                if (!isQualifiedCandidate(queryHistory))
                    continue;

                if ("NATIVE".equals(queryHistory.getEngineType())) {
                    numOfQueryHitIndex++;
                }
                String sqlPatternFromQuery = queryHistory.getSqlPattern();
                // get string reference from OverallStatus to avoid a big waste of String object
                String sqlPattern = copiedOverallStatus.getSqlPatterns().getOrDefault(sqlPatternFromQuery,
                        sqlPatternFromQuery);
                newStatus.updateFrequency(sqlPattern);

                if (FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .contains(sqlPattern)) {
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
            updateRelatedMetadata(candidates, endTime, numOfQueryHitIndex, overallQueryNum);
        }

        private Set<FavoriteQuery> getCandidatesByFrequencyRule() {
            FavoriteRule freqRule = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getByName(FavoriteRule.FREQUENCY_RULE_NAME);
            Preconditions.checkArgument(freqRule != null);

            if (!freqRule.isEnabled())
                return Sets.newHashSet();
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
            return getCandidates(distinctFreqMap, topK);
        }

        private Set<FavoriteQuery> getCandidates(Map<Integer, Set<String>> sqlPatternsMap, int topK) {
            Set<FavoriteQuery> candidates = Sets.newHashSet();

            if (topK < 1)
                return candidates;

            List<Integer> orderingResult = Ordering.natural().greatestOf(sqlPatternsMap.keySet(), topK);

            for (int frequency : orderingResult) {
                for (String sqlPattern : sqlPatternsMap.get(frequency)) {
                    FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern);
                    favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
                    candidates.add(favoriteQuery);
                }
            }

            return candidates;
        }
    }

    private boolean isQualifiedCandidate(QueryHistory queryHistory) {
        if (!QueryUtil.isSelectStatement(queryHistory.getSqlPattern()))
            return false;

        return !queryHistory.isException() || QueryHistory.NO_REALIZATION_FOUND_ERROR.equals(queryHistory.getErrorType());
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
            if (matchSingleRule(rule, queryHistory))
                return true;
        }

        return false;
    }

    private boolean matchSingleRule(FavoriteRule rule, QueryHistory queryHistory) {
        if (rule.getName().equals(FavoriteRule.SUBMITTER_RULE_NAME)) {
            for (FavoriteRule.Condition submitterCond : (List<FavoriteRule.Condition>) (List<?>) rule.getConds()) {
                if (queryHistory.getQuerySubmitter().equals(submitterCond.getRightThreshold()))
                    return true;
            }
        }

        if (rule.getName().equals(FavoriteRule.SUBMITTER_GROUP_RULE_NAME)) {
            Set<String> userGroups = getUserGroups(queryHistory.getQuerySubmitter());
            for (FavoriteRule.Condition userGroupCond : (List<FavoriteRule.Condition>) (List<?>) rule.getConds()) {
                if (userGroups.contains(userGroupCond.getRightThreshold()))
                    return true;
            }
        }

        if (rule.getName().equals(FavoriteRule.DURATION_RULE_NAME)) {
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(rule.getConds()));
            FavoriteRule.Condition durationCond = (FavoriteRule.Condition) rule.getConds().get(0);
            if (queryHistory.getDuration() >= Long.valueOf(durationCond.getLeftThreshold()) * 1000L
                    && queryHistory.getDuration() <= Long.valueOf(durationCond.getRightThreshold()) * 1000L)
                return true;
        }

        return false;
    }

    private Set<String> getUserGroups(String userName) {
        return KylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).getUserGroups(userName);
    }

    public void scheduleImmediately() {
        autoFavoriteScheduler.schedule(new AutoFavoriteRunner(), 1, TimeUnit.SECONDS);
        updateFavoriteScheduler.schedule(new UpdateUsageStatisticsRunner(project), 10L, TimeUnit.SECONDS);
    }

    public boolean hasStarted() {
        return this.hasStarted;
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
        // duplicate sql pattern map, in order to avoid waste of String objects
        private Map<String, String> sqlPatterns = Maps.newHashMap();
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
            sqlPatterns.put(sqlPattern, sqlPattern);
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
                this.sqlPatterns.put(sqlPattern, sqlPattern);
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
                        this.sqlPatterns.remove(sqlPattern);
                        continue;
                    }
                }

                this.sqlPatternFreqMap.put(sqlPattern, currentFreq);
                this.sqlPatterns.put(sqlPattern, sqlPattern);
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
