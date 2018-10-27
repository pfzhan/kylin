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
package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.QueryFilterRequest;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffsetManager;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.event.model.AccelerateEvent;

@Component("favoriteQueryService")
public class FavoriteQueryService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteQueryService.class);

    private Map<String, Integer> ignoreCountMap = Maps.newHashMap();

    @Autowired
    @Qualifier("queryHistoryService")
    QueryHistoryService queryHistoryService;

    private FavoriteQueryJDBCDao favoriteQueryJDBCDao = FavoriteQueryJDBCDao.getInstance(getConfig());
    private QueryHistoryTimeOffsetManager queryHistoryTimeOffsetManager = QueryHistoryTimeOffsetManager
            .getInstance(getConfig());
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private AutoMarkFavoriteRunner autoMarkFavoriteRunner = new AutoMarkFavoriteRunner();

    private QueryHistoryTimeOffset queryHistoryTimeOffset;
    private TreeSet<FrequencyStatus> frequencyStatuses;
    private FrequencyStatus overAllStatus;

    private static int frequencyTimeWindow = 24;
    private static int overAllFreqStatusSize = frequencyTimeWindow * 60;
    private static long fetchQueryHistoryGapTime = 60 * 1000L;

    @PostConstruct
    void init() throws IOException {
        if (FavoriteQueryJDBCDao.sqlPatternHashSet == null)
            favoriteQueryJDBCDao.initializeSqlPatternSet();
        queryHistoryTimeOffset = queryHistoryTimeOffsetManager.get();
        if (queryHistoryTimeOffset == null)
            queryHistoryTimeOffset = new QueryHistoryTimeOffset(0, 0);
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    initFrequencyStatus();
                } catch (IOException e) {
                    logger.error("init frequency status error", e);
                }
            }
        }, 0, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(autoMarkFavoriteRunner, 0, getConfig().getAutoMarkFavoriteInterval(),
                TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(new CollectFavoriteStatisticsRunner(), 0,
                getConfig().getFavoriteStatisticsCollectionInterval(), TimeUnit.SECONDS);
    }

    private void initFrequencyStatus() throws IOException {
        frequencyStatuses = new TreeSet<>();
        overAllStatus = new FrequencyStatus();

        if (queryHistoryTimeOffset.getAutoMarkTimeOffset() == 0) {
            queryHistoryTimeOffset.setAutoMarkTimeOffset(System.currentTimeMillis());
        }

        if (queryHistoryTimeOffset.getFavoriteQueryUpdateTimeOffset() == 0) {
            queryHistoryTimeOffset.setFavoriteQueryUpdateTimeOffset(System.currentTimeMillis());
        }

        long lastAutoMarkTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();
        long startTime = lastAutoMarkTime - overAllFreqStatusSize * fetchQueryHistoryGapTime;
        long endTime = startTime + fetchQueryHistoryGapTime;

        // past 24hr
        for (int i = 0; i < overAllFreqStatusSize; i++) {
            List<QueryHistory> queryHistories = queryHistoryService.getQueryHistories(startTime, endTime);

            FrequencyStatus frequencyStatus = new FrequencyStatus(startTime);

            for (QueryHistory queryHistory : queryHistories) {
                frequencyStatus.updateFrequency(queryHistory.getProject(), queryHistory.getSqlPattern());
                lastAutoMarkTime = queryHistory.getInsertTime();
            }

            updateOverallFrequencyStatus(frequencyStatus);

            startTime = endTime;
            endTime += fetchQueryHistoryGapTime;
        }

        queryHistoryTimeOffset.setAutoMarkTimeOffset(lastAutoMarkTime);
        queryHistoryTimeOffsetManager.save(queryHistoryTimeOffset);
    }

    public void manualFavorite(FavoriteRequest request) throws PersistentException {
        final String project = request.getProject();
        final String sqlPattern = request.getSqlPattern();
        final int sqlPatternHash = sqlPattern.hashCode();
        long queryTime = request.getQueryTime();
        String queryStatus = request.getQueryStatus();

        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));

        FavoriteQuery existFavorite = favoriteQueryJDBCDao.getFavoriteQuery(sqlPattern.hashCode(), project);
        if (existFavorite != null && !existFavorite.getStatus().equals(FavoriteQueryStatusEnum.WAITING))
            return;

        if (existFavorite == null) {
            final FavoriteQuery newfavoriteQuery = new FavoriteQuery(sqlPattern, sqlPatternHash, project);
            newfavoriteQuery.setLastQueryTime(queryTime);
            newfavoriteQuery.setStatus(FavoriteQueryStatusEnum.ACCELERATING);

            if (queryStatus.equals(QueryHistory.QUERY_HISTORY_SUCCEEDED))
                newfavoriteQuery.setSuccessCount(1);
            favoriteQueryJDBCDao.batchInsert(Lists.newArrayList(newfavoriteQuery));
        } else {
            existFavorite.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
            favoriteQueryJDBCDao.batchUpdateStatus(Lists.newArrayList(existFavorite));
        }

        post(project, Lists.newArrayList(sqlPattern), true);

        // update sql pattern hash set
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                Set<Integer> sqlPatternInProj = FavoriteQueryJDBCDao.sqlPatternHashSet.get(project);
                if (sqlPatternInProj == null)
                    sqlPatternInProj = new HashSet<>();

                sqlPatternInProj.add(sqlPatternHash);

                FavoriteQueryJDBCDao.sqlPatternHashSet.put(project, sqlPatternInProj);
            }
        }, 0, TimeUnit.SECONDS);
    }

    private void internalFavorite(final Set<FavoriteQuery> favoriteQueries) {
        List<FavoriteQuery> favoriteQueriesToInsert = Lists.newArrayList();
        Map<String, Set<Integer>> sqlPatternToUpdate = Maps.newHashMap();

        for (FavoriteQuery favoriteQuery : favoriteQueries) {
            String project = favoriteQuery.getProject();
            final int sqlPatternHash = favoriteQuery.getSqlPatternHash();
            if (FavoriteQueryJDBCDao.isInDatabase(project, sqlPatternHash))
                continue;

            favoriteQueriesToInsert.add(favoriteQuery);

            Set<Integer> sqlPatternHashSet = sqlPatternToUpdate.get(project);

            if (sqlPatternHashSet == null)
                sqlPatternHashSet = new HashSet<>();

            sqlPatternHashSet.add(sqlPatternHash);
            sqlPatternToUpdate.put(project, sqlPatternHashSet);
        }

        favoriteQueryJDBCDao.batchInsert(favoriteQueriesToInsert);

        // update sql pattern hash set
        for (Map.Entry<String, Set<Integer>> entry : sqlPatternToUpdate.entrySet()) {
            String project = entry.getKey();
            Set<Integer> sqlPatternInProj = FavoriteQueryJDBCDao.sqlPatternHashSet.get(project);
            if (sqlPatternInProj == null)
                sqlPatternInProj = entry.getValue();
            else
                sqlPatternInProj.addAll(entry.getValue());

            FavoriteQueryJDBCDao.sqlPatternHashSet.put(project, sqlPatternInProj);
        }
    }

    private void autoMark() throws IOException {
        // scan query histories by the interval of 60 seconds
        boolean isMaxTimeUpdated = false;
        long maxTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();
        long currentTime = System.currentTimeMillis();
        Set<FavoriteQuery> candidates = new HashSet<>();
        long startTime = maxTime;
        long endTime = startTime + fetchQueryHistoryGapTime;
        while (endTime < currentTime) {
            List<QueryHistory> queryHistories = queryHistoryService.getQueryHistories(startTime, endTime);

            FrequencyStatus newStatus = new FrequencyStatus(startTime);

            for (QueryHistory queryHistory : queryHistories) {
                String project = queryHistory.getProject();
                String sqlPattern = queryHistory.getSqlPattern();

                if (matchRuleBySingleRecord(queryHistory)) {
                    final FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPattern.hashCode(), project);
                    favoriteQuery.setLastQueryTime(queryHistory.getQueryTime());
                    if (!queryHistory.isException())
                        favoriteQuery.setSuccessCount(1);
                    candidates.add(favoriteQuery);
                }

                newStatus.updateFrequency(project, sqlPattern);

                maxTime = queryHistory.getInsertTime();
                isMaxTimeUpdated = true;
            }

            updateOverallFrequencyStatus(newStatus);

            startTime = endTime;
            endTime += fetchQueryHistoryGapTime;
        }

        addCandidatesByFrequencyRule(candidates);

        // insert candidates to favorite query
        internalFavorite(candidates);

        if (isMaxTimeUpdated)
            queryHistoryTimeOffset.setAutoMarkTimeOffset(maxTime);
        queryHistoryTimeOffsetManager.save(queryHistoryTimeOffset);
    }

    private void addCandidatesByFrequencyRule(Set<FavoriteQuery> candidates) {
        for (Map.Entry<String, Map<String, Integer>> entry : overAllStatus.getSqlPatternFreqMap().entrySet()) {
            String project = entry.getKey();

            QueryFilterRule freqRule = getQueryFilterRule(project, QueryFilterRule.FREQUENCY_RULE_NAME);

            // when project is deleted
            if (freqRule == null)
                continue;

            if (!freqRule.isEnabled())
                continue;

            if (freqRule.getConds() == null || freqRule.getConds().isEmpty())
                throw new IllegalArgumentException(String.format("Rule %s does not have conditions", freqRule.getId()));

            Map<Integer, Set<String>> distinctFreqMap = Maps.newHashMap();

            for (Map.Entry<String, Integer> entryInProject : entry.getValue().entrySet()) {
                final String sqlPattern = entryInProject.getKey();
                int keyOfDistinctFreqMap = entryInProject.getValue();
                Set<String> sqlPatternSet = distinctFreqMap.get(keyOfDistinctFreqMap);
                if (sqlPatternSet == null)
                    sqlPatternSet = new HashSet<>();

                sqlPatternSet.add(sqlPattern);
                distinctFreqMap.put(keyOfDistinctFreqMap, sqlPatternSet);
            }

            int topK = (int) Math
                    .floor(distinctFreqMap.size() * Float.valueOf(freqRule.getConds().get(0).getRightThreshold()));
            addCandidates(candidates, distinctFreqMap, topK, project);
        }
    }

    private void addCandidates(Set<FavoriteQuery> candidates, Map<Integer, Set<String>> sqlPatternsMap, int topK,
            String project) {
        if (topK < 1)
            return;

        List<Integer> orderingResult = Ordering.natural().greatestOf(sqlPatternsMap.keySet(), topK);

        for (int frequency : orderingResult) {
            for (String sqlPattern : sqlPatternsMap.get(frequency)) {
                FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPattern.hashCode(), project);
                candidates.add(favoriteQuery);
            }
        }
    }

    private void updateOverallFrequencyStatus(FrequencyStatus newStatus) {
        frequencyStatuses.add(newStatus);

        // remove status beyond 24hrs
        if (frequencyStatuses.size() > overAllFreqStatusSize) {
            FrequencyStatus removedStatus = frequencyStatuses.pollFirst();
            overAllStatus.removeStatus(removedStatus);
        }

        // add status
        overAllStatus.addStatus(newStatus);
    }

    private boolean matchRuleBySingleRecord(QueryHistory queryHistory) {
        List<QueryFilterRule> rules = getQueryFilterRuleManager(queryHistory.getProject()).getAllEnabled();

        for (QueryFilterRule rule : rules) {
            if (rule.getConds() == null || rule.getConds().isEmpty())
                throw new IllegalArgumentException(String.format("Rule %s does not have conditions", rule.getName()));

            if (rule.getName().equals(QueryFilterRule.SUBMITTER_RULE_NAME)) {
                for (QueryFilterRule.QueryHistoryCond submitterCond : rule.getConds()) {
                    if (queryHistory.getQuerySubmitter().equals(submitterCond.getRightThreshold()))
                        return true;
                }
            }

            if (rule.getName().equals(QueryFilterRule.DURATION_RULE_NAME)) {
                QueryFilterRule.QueryHistoryCond durationCond = rule.getConds().get(0);
                if (queryHistory.getDuration() >= Long.valueOf(durationCond.getLeftThreshold()) * 1000L
                        && queryHistory.getDuration() <= Long.valueOf(durationCond.getRightThreshold()) * 1000L)
                    return true;
            }
        }

        return false;
    }

    public List<FavoriteQuery> getFavoriteQueriesByPage(String project, int limit, int offset) {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        return favoriteQueryJDBCDao.getByPage(project, limit, offset);
    }

    private int getOptimizedModelNum(String project, String[] sqls) {
        int optimizedModelNum = 0;
        NSmartMaster smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.renameModel();

        List<NSmartContext.NModelContext> modelContexts = Lists.newArrayList();

        for (NSmartContext.NModelContext modelContext : smartMaster.getContext().getModelContexts()) {
            if ((modelContext.getOrigModel() == null && modelContext.getTargetModel() != null)
                    || !modelContext.getOrigModel().equals(modelContext.getTargetModel())) {
                optimizedModelNum++;
            } else
                modelContexts.add(modelContext);
        }

        if (optimizedModelNum == smartMaster.getContext().getModelContexts().size())
            return optimizedModelNum;

        smartMaster.selectCubePlan();
        smartMaster.optimizeCubePlan();

        for (NSmartContext.NModelContext modelContext : modelContexts) {
            List<NCuboidLayout> origCuboidLayouts = Lists.newArrayList();
            List<NCuboidLayout> targetCuboidLayouts = Lists.newArrayList();

            if (modelContext.getOrigCubePlan() != null)
                origCuboidLayouts = modelContext.getOrigCubePlan().getAllCuboidLayouts();

            if (modelContext.getTargetCubePlan() != null)
                targetCuboidLayouts = modelContext.getTargetCubePlan().getAllCuboidLayouts();

            if (!doTwoCubiodLayoutsEqual(origCuboidLayouts, targetCuboidLayouts))
                optimizedModelNum++;
        }

        return optimizedModelNum;
    }

    private boolean doTwoCubiodLayoutsEqual(List<NCuboidLayout> origCuboidLayouts,
            List<NCuboidLayout> targetCuboidLayouts) {
        if (origCuboidLayouts.size() != targetCuboidLayouts.size())
            return false;

        for (int i = 0; i < origCuboidLayouts.size(); i++) {
            if (origCuboidLayouts.get(i).getId() != targetCuboidLayouts.get(i).getId())
                return false;
        }

        return true;
    }

    public HashMap<String, Object> getAccelerateTips(String project) {
        HashMap<String, Object> data = Maps.newHashMap();
        List<String> unAcceleratedSqls = favoriteQueryJDBCDao.getUnAcceleratedSqlPattern(project);
        int optimizedModelNum = 0;

        if (!unAcceleratedSqls.isEmpty()) {
            optimizedModelNum = getOptimizedModelNum(project,
                    unAcceleratedSqls.toArray(new String[unAcceleratedSqls.size()]));
        }

        data.put("size", unAcceleratedSqls.size());
        data.put("reach_threshold", false);
        data.put("optimized_model_num", optimizedModelNum);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        int ignoreCount = 1;
        if (ignoreCountMap.containsKey(project))
            ignoreCount = ignoreCountMap.get(project);
        else
            ignoreCountMap.put(project, 1);

        if (unAcceleratedSqls.size() >= projectInstance.getConfig().getFavoriteQueryAccelerateThreshold() * ignoreCount)
            data.put("reach_threshold", true);

        return data;
    }

    public void acceptAccelerate(String project, int accelerateSize) throws PersistentException {
        List<String> sqlPatterns = Lists.newArrayList();
        List<String> unAcceleratedSqlPattern = favoriteQueryJDBCDao.getUnAcceleratedSqlPattern(project);
        if (accelerateSize > unAcceleratedSqlPattern.size()) {
            throw new IllegalArgumentException(
                    String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), accelerateSize));
        }

        int batchAccelerateSize = getConfig().getFavoriteAccelerateBatchSize();
        int count = 1;

        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();

        for (String sqlPattern : unAcceleratedSqlPattern) {
            sqlPatterns.add(sqlPattern);
            FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPattern.hashCode(), project);
            favoriteQuery.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
            favoriteQueries.add(favoriteQuery);

            if (count % batchAccelerateSize == 0) {
                favoriteQueryJDBCDao.batchUpdateStatus(favoriteQueries);
                post(project, sqlPatterns, true);

                favoriteQueries.clear();
                sqlPatterns.clear();
            }

            count++;
        }

        if (ignoreCountMap.containsKey(project))
            ignoreCountMap.put(project, 1);
    }

    public void ignoreAccelerate(String project) {
        int ignoreCount = ignoreCountMap.get(project);
        ignoreCount++;
        ignoreCountMap.put(project, ignoreCount);
    }

    Map<String, Integer> getIgnoreCountMap() {
        return ignoreCountMap;
    }

    void post(String project, List<String> sqls, boolean favoriteMark) throws PersistentException {
        AccelerateEvent accelerateEvent = new AccelerateEvent();
        accelerateEvent.setFavoriteMark(favoriteMark);
        accelerateEvent.setProject(project);
        accelerateEvent.setSqlPatterns(sqls);
        accelerateEvent.setApproved(true);
        getEventManager(project).post(accelerateEvent);
    }

    public Map<String, Object> getFrequencyRule(String project) {
        QueryFilterRule frequencyRule = getQueryFilterRule(project, QueryFilterRule.FREQUENCY_RULE_NAME);
        if (frequencyRule == null || frequencyRule.getConds().isEmpty())
            throw new NotFoundException(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    QueryFilterRule.FREQUENCY_RULE_NAME));

        Map<String, Object> result = Maps.newHashMap();
        result.put(QueryFilterRule.ENABLE, frequencyRule.isEnabled());
        result.put("freqValue", Float.valueOf(frequencyRule.getConds().get(0).getRightThreshold()));

        return result;
    }

    public Map<String, Object> getSubmitterRule(String project) {
        QueryFilterRule submitterRule = getQueryFilterRule(project, QueryFilterRule.SUBMITTER_RULE_NAME);
        if (submitterRule == null || submitterRule.getConds().isEmpty())
            throw new NotFoundException(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    QueryFilterRule.SUBMITTER_RULE_NAME));

        Map<String, Object> result = Maps.newHashMap();
        result.put(QueryFilterRule.ENABLE, submitterRule.isEnabled());
        List<String> users = Lists.newArrayList();
        for (QueryFilterRule.QueryHistoryCond cond : submitterRule.getConds()) {
            users.add(cond.getRightThreshold());
        }
        result.put("users", users);
        result.put("groups", Lists.newArrayList());

        return result;
    }

    public Map<String, Object> getDurationRule(String project) {
        QueryFilterRule durationRule = getQueryFilterRule(project, QueryFilterRule.DURATION_RULE_NAME);
        if (durationRule == null || durationRule.getConds().isEmpty())
            throw new NotFoundException(
                    String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), QueryFilterRule.DURATION_RULE_NAME));

        Map<String, Object> result = Maps.newHashMap();
        result.put(QueryFilterRule.ENABLE, durationRule.isEnabled());
        result.put("durationValue", Lists.newArrayList(Long.valueOf(durationRule.getConds().get(0).getLeftThreshold()),
                Long.valueOf(durationRule.getConds().get(0).getRightThreshold())));

        return result;
    }

    private QueryFilterRule getQueryFilterRule(String project, String ruleName) {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        Preconditions.checkArgument(ruleName != null && StringUtils.isNotEmpty(ruleName));

        return getQueryFilterRuleManager(project).getByName(ruleName);
    }

    public void updateQueryFilterRule(QueryFilterRequest request, String ruleName) throws IOException {
        Preconditions.checkArgument(request.getProject() != null && StringUtils.isNotEmpty(request.getProject()));

        QueryFilterRuleManager manager = getQueryFilterRuleManager(request.getProject());
        QueryFilterRule rule = manager.getByName(ruleName);

        if (rule == null || rule.getConds().isEmpty())
            throw new IllegalArgumentException(
                    String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), ruleName));

        rule.setEnabled(request.isEnable());

        List<QueryFilterRule.QueryHistoryCond> conds = Lists.newArrayList();

        switch (ruleName) {
        case QueryFilterRule.FREQUENCY_RULE_NAME:
            QueryFilterRule.QueryHistoryCond freqCond = rule.getConds().get(0);
            freqCond.setRightThreshold(request.getFreqValue());
            conds.add(freqCond);
            break;
        case QueryFilterRule.SUBMITTER_RULE_NAME:
            for (String user : request.getUsers()) {
                conds.add(new QueryFilterRule.QueryHistoryCond(QueryFilterRule.FREQUENCY, null, user));
            }
            break;
        case QueryFilterRule.DURATION:
            if (request.getDurationValue().length < 2)
                throw new IllegalArgumentException("Duration rule should have both left threshold and right threshold");
            QueryFilterRule.QueryHistoryCond durationCond = rule.getConds().get(0);
            durationCond.setLeftThreshold(request.getDurationValue()[0]);
            durationCond.setRightThreshold(request.getDurationValue()[1]);
            conds.add(durationCond);
            break;
        default:
            break;
        }

        rule.setConds(conds);
        manager.save(rule);
        scheduler.schedule(autoMarkFavoriteRunner, 0, TimeUnit.SECONDS);
    }

    private class AutoMarkFavoriteRunner implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(AutoMarkFavoriteRunner.class);

        @Override
        public void run() {
            logger.info("auto mark favorite runner begins");
            try {
                autoMark();
            } catch (Exception e) {
                logger.error("Error caught when auto mark favorite", e);
            }
        }
    }

    private class CollectFavoriteStatisticsRunner implements Runnable {

        @Override
        public void run() {
            boolean isMaxTimeUpdated = false;
            long maxTime = System.currentTimeMillis();
            Map<String, Map<Integer, FavoriteQuery>> favoritesAboutToUpdate = Maps.newHashMap();
            List<QueryHistory> queryHistories = queryHistoryService
                    .getQueryHistories(queryHistoryTimeOffset.getFavoriteQueryUpdateTimeOffset(), maxTime);

            for (QueryHistory queryHistory : queryHistories) {
                updateFavoriteStatistics(queryHistory, favoritesAboutToUpdate);
                maxTime = queryHistory.getInsertTime();
                isMaxTimeUpdated = true;
            }

            List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
            for (Map.Entry<String, Map<Integer, FavoriteQuery>> favorites : favoritesAboutToUpdate.entrySet()) {
                for (Map.Entry<Integer, FavoriteQuery> favoritesInProj : favorites.getValue().entrySet()) {
                    favoriteQueries.add(favoritesInProj.getValue());
                }
            }

            favoriteQueryJDBCDao.batchUpdate(favoriteQueries);

            if (isMaxTimeUpdated)
                queryHistoryTimeOffset.setFavoriteQueryUpdateTimeOffset(maxTime);
            try {
                queryHistoryTimeOffsetManager.save(queryHistoryTimeOffset);
            } catch (IOException e) {
                logger.error("Error caught when collecting favorite statistics", e);
            }
        }

        private void updateFavoriteStatistics(QueryHistory queryHistory,
                Map<String, Map<Integer, FavoriteQuery>> favoritesAboutToUpdate) {
            String project = queryHistory.getProject();
            String sqlPattern = queryHistory.getSqlPattern();
            final int sqlPatternHash = sqlPattern.hashCode();

            if (!FavoriteQueryJDBCDao.isInDatabase(project, sqlPatternHash))
                return;

            Map<Integer, FavoriteQuery> mapInProj = favoritesAboutToUpdate.get(project);

            if (mapInProj != null && mapInProj.containsKey(sqlPatternHash)) {
                FavoriteQuery favoriteQuery = mapInProj.get(sqlPatternHash);
                favoriteQuery.increaseTotalCountByOne();
                if (!queryHistory.isException())
                    favoriteQuery.increaseSuccessCountByOne();
                favoriteQuery.increaseTotalDuration(queryHistory.getDuration());
                mapInProj.put(sqlPatternHash, favoriteQuery);
            } else {
                FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPatternHash, project,
                        queryHistory.getQueryTime(), 1, queryHistory.getDuration());
                if (!queryHistory.isException())
                    favoriteQuery.setSuccessCount(1);

                if (mapInProj == null)
                    mapInProj = new HashMap<>();

                mapInProj.put(sqlPatternHash, favoriteQuery);
            }

            favoritesAboutToUpdate.put(project, mapInProj);
        }
    }

    private class FrequencyStatus implements Comparable<FrequencyStatus> {
        // key of the outer hashmap is the project, key of the inner hashmap is the sql pattern
        private Map<String, Map<String, Integer>> sqlPatternFreqMap = Maps.newHashMap();
        private long addTime;

        public FrequencyStatus() {

        }

        public FrequencyStatus(long currentTime) {
            this.addTime = currentTime;
        }

        public void updateFrequency(final String project, final String sqlPattern) {
            Map<String, Integer> sqlPatternFreqInProject = sqlPatternFreqMap.get(project);
            if (sqlPatternFreqInProject == null) {
                sqlPatternFreqInProject = new HashMap<>();
                sqlPatternFreqInProject.put(sqlPattern, 1);
            } else {
                Integer frequency = sqlPatternFreqInProject.get(sqlPattern);
                if (frequency == null)
                    frequency = 1;
                else
                    frequency++;
                sqlPatternFreqInProject.put(sqlPattern, frequency);
            }

            sqlPatternFreqMap.put(project, sqlPatternFreqInProject);
        }

        public void addStatus(final FrequencyStatus newStatus) {
            for (Map.Entry<String, Map<String, Integer>> newFreqMap : newStatus.getSqlPatternFreqMap().entrySet()) {
                String project = newFreqMap.getKey();

                Map<String, Integer> sqlPatternFreqInProj = this.sqlPatternFreqMap.get(project);

                if (sqlPatternFreqInProj == null) {
                    sqlPatternFreqInProj = newFreqMap.getValue();
                } else {
                    for (Map.Entry<String, Integer> newFreq : newFreqMap.getValue().entrySet()) {
                        String sqlPattern = newFreq.getKey();
                        Integer frequency = sqlPatternFreqInProj.get(sqlPattern);
                        if (frequency == null) {
                            frequency = newFreq.getValue();
                        } else {
                            frequency += newFreq.getValue();
                        }
                        sqlPatternFreqInProj.put(sqlPattern, frequency);
                    }
                }

                this.sqlPatternFreqMap.put(project, sqlPatternFreqInProj);
            }
        }

        public void removeStatus(final FrequencyStatus removedStatus) {
            for (Map.Entry<String, Map<String, Integer>> removedFreqMap : removedStatus.getSqlPatternFreqMap()
                    .entrySet()) {
                String project = removedFreqMap.getKey();
                Map<String, Integer> sqlPatternFreqInProj = this.sqlPatternFreqMap.get(project);

                if (sqlPatternFreqInProj != null) {
                    for (Map.Entry<String, Integer> removedFreq : removedFreqMap.getValue().entrySet()) {
                        String sqlPattern = removedFreq.getKey();
                        Integer frequency = sqlPatternFreqInProj.get(sqlPattern);
                        if (frequency != null) {
                            frequency -= removedFreq.getValue();
                            sqlPatternFreqInProj.put(sqlPattern, frequency);
                        }
                    }

                    this.sqlPatternFreqMap.put(project, sqlPatternFreqInProj);
                }
            }
        }

        public Map<String, Map<String, Integer>> getSqlPatternFreqMap() {
            return sqlPatternFreqMap;
        }

        public long getAddTime() {
            return addTime;
        }

        @Override
        public int compareTo(FrequencyStatus o) {
            if (this.addTime == o.getAddTime())
                return 0;

            return this.addTime > o.getAddTime() ? 1 : -1;
        }
    }
}
