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

import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryResponse;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
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
import io.kyligence.kap.event.model.AccelerateEvent;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffset;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;

@Component("favoriteQueryService")
public class FavoriteQueryService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteQueryService.class);

    private Map<String, Integer> ignoreCountMap = Maps.newHashMap();

    @Autowired
    @Qualifier("queryHistoryService")
    QueryHistoryService queryHistoryService;

    @Autowired
    @Qualifier("favoriteRuleService")
    FavoriteRuleService favoriteRuleService;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private AutoMarkFavoriteRunner autoMarkFavoriteRunner = new AutoMarkFavoriteRunner();

    private QueryHistoryTimeOffset queryHistoryTimeOffset;
    private TreeSet<FrequencyStatus> frequencyStatuses = new TreeSet<>();
    private FrequencyStatus overAllStatus = new FrequencyStatus();

    private static int frequencyTimeWindow = 24;
    private static int overAllFreqStatusSize = frequencyTimeWindow * 60;
    private long fetchQueryHistoryGapTime;
    // handles the case when the actual time of inserting to influx database is later than recorded time
    private int backwardShiftTime;

    public FavoriteQueryService() {
        fetchQueryHistoryGapTime = KylinConfig.getInstanceFromEnv().getQueryHistoryScanPeriod();
        backwardShiftTime = KapConfig.getInstanceFromEnv().getInfluxDBFlushDuration() * 2;
        try {
            queryHistoryTimeOffset = getQHTimeOffsetManager().get();
        } catch (IOException e) {
            throw new RuntimeException("Caught errors when get query history time offset: ", e);
        }
    }

    @PostConstruct
    void init() {
        getFavoriteQueryDao().initializeSqlPatternSet();
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

    void initFrequencyStatus() throws IOException {
        if (queryHistoryTimeOffset.getAutoMarkTimeOffset() == 0) {
            queryHistoryTimeOffset.setAutoMarkTimeOffset(System.currentTimeMillis() - backwardShiftTime);
        }

        if (queryHistoryTimeOffset.getFavoriteQueryUpdateTimeOffset() == 0) {
            queryHistoryTimeOffset.setFavoriteQueryUpdateTimeOffset(System.currentTimeMillis() - backwardShiftTime);
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
            }

            updateOverallFrequencyStatus(frequencyStatus);

            startTime = endTime;
            endTime += fetchQueryHistoryGapTime;
        }

        getQHTimeOffsetManager().save(queryHistoryTimeOffset);
    }

    void insertToDaoAndAccelerateForWhitelistChannel(Set<String> sqlPatterns, String project) throws IOException, PersistentException {
        List<String> sqlsToAccelerate = Lists.newArrayList();
        List<FavoriteQuery> favoriteQueriesToInsert = Lists.newArrayList();

        for (String sqlPattern : sqlPatterns) {
            int sqlPatternHash = sqlPattern.hashCode();
            FavoriteQuery existFavorite = getFavoriteQueryDao().getFavoriteQuery(sqlPatternHash, project);
            if (existFavorite != null && !existFavorite.getStatus().equals(FavoriteQueryStatusEnum.WAITING))
                continue;

            sqlsToAccelerate.add(sqlPattern);

            if (existFavorite == null) {
                FavoriteQuery newFavoriteQuery = new FavoriteQuery(sqlPattern, sqlPatternHash, project);
                newFavoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_WHITE_LIST);
                favoriteQueriesToInsert.add(newFavoriteQuery);
            }
        }

        getFavoriteQueryDao().batchInsert(favoriteQueriesToInsert);
        // accelerate sqls right now
        if (!sqlsToAccelerate.isEmpty())
            post(project, sqlsToAccelerate, true);
    }

    public void manualFavorite(FavoriteRequest request) throws IOException {
        Preconditions.checkArgument(request.getProject() != null && StringUtils.isNotEmpty(request.getProject()));
        if (QueryHistory.QUERY_HISTORY_FAILED.equals(request.getQueryStatus()))
            return;

        String sqlPattern = request.getSqlPattern();
        Set<String> sqlPatterns = new HashSet<>();
        sqlPatterns.add(sqlPattern);
        favoriteRuleService.appendSqlToWhitelist(request.getSql(), sqlPattern.hashCode(), request.getProject());
        favoriteForWhitelistChannel(sqlPatterns, request.getProject());
    }

    public void favoriteForWhitelistChannel(Set<String> sqlPatterns, String project) {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    insertToDaoAndAccelerateForWhitelistChannel(sqlPatterns, project);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }, 0, TimeUnit.SECONDS);
    }

    private void internalFavorite(final Set<FavoriteQuery> favoriteQueries) throws IOException, PersistentException {
        getFavoriteQueryDao().batchInsert(Lists.newArrayList(favoriteQueries));

        Map<String, Set<Integer>> sqlPatternHashSet = FavoriteQueryJDBCDao.getSqlPatternHashSet();
        NProjectManager projectManager = getProjectManager();
        //acceptAccelerate without apply
        for (String project : sqlPatternHashSet.keySet()) {
            ProjectInstance projectInstance = projectManager.getProject(project);
            if ((projectInstance.getConfig().getFavoriteQueryAccelerateThresholdBatchEnabled())
                    && projectInstance.getConfig().getFavoriteQueryAccelerateThresholdAutoApply()) {
                accelerateAllUnAcceleratedSqlPattern(project);
            }
        }
    }

    long getSystemTime() {
        return System.currentTimeMillis();
    }

    private void accelerateAllUnAcceleratedSqlPattern(String project) throws IOException, PersistentException {
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        int unAcceleratedSqlPatternSize = getFavoriteQueryDao().getUnAcceleratedSqlPattern(project).size();
        if (unAcceleratedSqlPatternSize < projectInstance.getConfig().getFavoriteQueryAccelerateThreshold()) {
            return;
        } else {
            acceptAccelerate(project, unAcceleratedSqlPatternSize);
        }
    }

    private void autoMark() throws IOException, PersistentException {
        // scan query histories by the interval of 60 seconds
        long startTime = queryHistoryTimeOffset.getAutoMarkTimeOffset();
        long endTime = startTime + fetchQueryHistoryGapTime;
        long maxTime = getSystemTime() - backwardShiftTime;

        Set<FavoriteQuery> candidates = new HashSet<>();
        while (endTime <= maxTime) {
            List<QueryHistory> queryHistories = queryHistoryService.getQueryHistories(startTime, endTime);

            FrequencyStatus newStatus = new FrequencyStatus(startTime);

            for (QueryHistory queryHistory : queryHistories) {
                String project = queryHistory.getProject();
                String sqlPattern = queryHistory.getSqlPattern();

                if (queryHistory.isException())
                    continue;

                int sqlPatternHash = sqlPattern.hashCode();
                if (FavoriteQueryJDBCDao.isInDatabase(project, sqlPatternHash))
                    continue;

                if (isInBlacklist(sqlPatternHash, project))
                    continue;

                if (matchRuleBySingleRecord(queryHistory)) {
                    final FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPatternHash, project);
                    favoriteQuery.setLastQueryTime(queryHistory.getQueryTime());
                    favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
                    candidates.add(favoriteQuery);
                }

                newStatus.updateFrequency(project, sqlPattern);
            }

            updateOverallFrequencyStatus(newStatus);

            startTime = endTime;
            endTime += fetchQueryHistoryGapTime;
        }

        addCandidatesByFrequencyRule(candidates);

        // insert candidates to favorite query
        internalFavorite(candidates);

        queryHistoryTimeOffset.setAutoMarkTimeOffset(startTime);

        getQHTimeOffsetManager().save(queryHistoryTimeOffset);
    }

    void addCandidatesByFrequencyRule(Set<FavoriteQuery> candidates) {
        for (Map.Entry<String, Map<String, Integer>> entry : getOverAllStatus().getSqlPatternFreqMap().entrySet()) {
            String project = entry.getKey();

            FavoriteRule freqRule = favoriteRuleService.getFavoriteRule(project, FavoriteRule.FREQUENCY_RULE_NAME);

            // when project is deleted
            if (freqRule == null || !freqRule.isEnabled())
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

            FavoriteRule.Condition condition = (FavoriteRule.Condition) freqRule.getConds().get(0);

            int topK = (int) Math.floor(distinctFreqMap.size() * Float.valueOf(condition.getRightThreshold()));
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
                if (FavoriteQueryJDBCDao.isInDatabase(project, sqlPattern.hashCode()))
                    continue;
                FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPattern.hashCode(), project);
                favoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);
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

    boolean matchRuleBySingleRecord(QueryHistory queryHistory) {
        List<FavoriteRule> rules = getFavoriteRuleManager(queryHistory.getProject()).getAllEnabled();

        for (FavoriteRule rule : rules) {
            if (rule.getConds() == null || rule.getConds().isEmpty())
                throw new IllegalArgumentException(String.format("Rule %s does not have conditions", rule.getUuid()));

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

    private boolean isInBlacklist(int sqlPatternHash, String project) {
        FavoriteRule blacklist = favoriteRuleService.getFavoriteRule(project, FavoriteRule.BLACKLIST_NAME);
        List<FavoriteRule.AbstractCondition> conditions = blacklist.getConds();

        for (FavoriteRule.AbstractCondition condition : conditions) {
            if (sqlPatternHash == ((FavoriteRule.SQLCondition) condition).getSqlPatternHash())
                return true;
        }

        return false;
    }

    FrequencyStatus getOverAllStatus() {
        return overAllStatus;
    }

    TreeSet<FrequencyStatus> getFrequencyStatuses() {
        return frequencyStatuses;
    }

    public List<FavoriteQueryResponse> getFavoriteQueriesByPage(String project, int limit, int offset) {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        return getFavoriteQueryDao().getByPage(project, limit, offset);
    }

    public int getFavoriteQuerySize(String project) {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));

        Set<Integer> sqlPatternHashSetInProj = FavoriteQueryJDBCDao.getSqlPatternHashSet().get(project);
        if (sqlPatternHashSetInProj == null)
            return 0;

        return sqlPatternHashSetInProj.size();
    }

    private int getOptimizedModelNum(String project, String[] sqls) {
        int optimizedModelNum = 0;
        NSmartMaster smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();

        List<NSmartContext.NModelContext> modelContexts = Lists.newArrayList();

        for (NSmartContext.NModelContext modelContext : smartMaster.getContext().getModelContexts()) {
            // case in manual maintain type project and no model is selected
            if (modelContext.getOrigModel() == null && modelContext.getTargetModel() == null)
                continue;

            if ((modelContext.getOrigModel() == null && modelContext.getTargetModel() != null)
                    || !modelContext.getOrigModel().equals(modelContext.getTargetModel())) {
                optimizedModelNum++;
            } else
                modelContexts.add(modelContext);
        }

        if (modelContexts.isEmpty())
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

    public Map<String, Object> getAccelerateTips(String project) {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        Map<String, Object> data = Maps.newHashMap();
        List<String> unAcceleratedSqls = getUnAcceleratedSqlPattern(project);
        int optimizedModelNum = 0;

        data.put("size", unAcceleratedSqls.size());
        data.put("reach_threshold", false);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        int ignoreCount = 1;
        if (ignoreCountMap.containsKey(project))
            ignoreCount = ignoreCountMap.get(project);
        else
            ignoreCountMap.put(project, 1);

        if (unAcceleratedSqls.size() >= projectInstance.getConfig().getFavoriteQueryAccelerateThreshold() * ignoreCount) {
            data.put("reach_threshold", true);
            if (!unAcceleratedSqls.isEmpty()) {
                optimizedModelNum = getOptimizedModelNum(project,
                        unAcceleratedSqls.toArray(new String[unAcceleratedSqls.size()]));
            }
        }

        data.put("optimized_model_num", optimizedModelNum);

        return data;
    }

    List<String> getUnAcceleratedSqlPattern(String project) {
        return getFavoriteQueryDao().getUnAcceleratedSqlPattern(project);
    }

    public void acceptAccelerate(String project, int accelerateSize) throws PersistentException, IOException {
        List<String> sqlPatterns = Lists.newArrayList();
        List<String> unAcceleratedSqlPattern = getUnAcceleratedSqlPattern(project);
        if (accelerateSize > unAcceleratedSqlPattern.size()) {
            throw new IllegalArgumentException(
                    String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), accelerateSize));
        }

        int batchAccelerateSize = getConfig().getFavoriteAccelerateBatchSize();
        int count = 1;

        for (String sqlPattern : unAcceleratedSqlPattern.subList(0, accelerateSize)) {
            sqlPatterns.add(sqlPattern);

            if (count % batchAccelerateSize == 0) {
                post(project, sqlPatterns, true);
                sqlPatterns.clear();
            }

            count++;
        }

        if (!sqlPatterns.isEmpty()) {
            post(project, sqlPatterns, true);
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

    void post(String project, List<String> sqls, boolean favoriteMark) throws PersistentException, IOException {
        val master = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project, sqls.toArray(new String[0]));
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<String> models = Lists.newArrayList();
        master.analyzeSQLs();
        master.selectModel();
        for (NSmartContext.NModelContext modelContext : master.getContext().getModelContexts()) {
            val model = modelContext.getOrigModel();
            if (model == null) {
                continue;
            }
            val df = dataflowManager.getDataflowByModelName(model.getName());
            if (df.isReconstructing()) {
                throw new IllegalStateException("model " + model.getName() + " is reconstructing");
            }
            models.add(model.getName());
            dataflowManager.updateDataflow(df.getName(), copy -> copy.setReconstructing(true));
        }
        AccelerateEvent accelerateEvent = new AccelerateEvent();
        accelerateEvent.setFavoriteMark(favoriteMark);
        accelerateEvent.setProject(project);
        accelerateEvent.setSqlPatterns(sqls);
        accelerateEvent.setApproved(true);
        accelerateEvent.setModels(models);
        getEventManager(project).post(accelerateEvent);
    }

    public void scheduleAutoMark() {
        scheduler.schedule(autoMarkFavoriteRunner, 0, TimeUnit.SECONDS);
    }

    class AutoMarkFavoriteRunner implements Runnable {
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

    class CollectFavoriteStatisticsRunner implements Runnable {

        @Override
        public void run() {
            long endTime = getSystemTime() - backwardShiftTime;
            Map<String, Map<Integer, FavoriteQuery>> favoritesAboutToUpdate = Maps.newHashMap();
            List<QueryHistory> queryHistories = queryHistoryService
                    .getQueryHistories(queryHistoryTimeOffset.getFavoriteQueryUpdateTimeOffset(), endTime);

            for (QueryHistory queryHistory : queryHistories) {
                updateFavoriteStatistics(queryHistory, favoritesAboutToUpdate);
            }

            List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
            for (Map.Entry<String, Map<Integer, FavoriteQuery>> favorites : favoritesAboutToUpdate.entrySet()) {
                for (Map.Entry<Integer, FavoriteQuery> favoritesInProj : favorites.getValue().entrySet()) {
                    favoriteQueries.add(favoritesInProj.getValue());
                }
            }

            getFavoriteQueryDao().batchUpdate(favoriteQueries);
            queryHistoryTimeOffset.setFavoriteQueryUpdateTimeOffset(endTime);
            try {
                getQHTimeOffsetManager().save(queryHistoryTimeOffset);
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
                favoriteQuery.update(queryHistory);
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

    @Getter
    @Setter
    class FrequencyStatus implements Comparable<FrequencyStatus> {
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

        @Override
        public int compareTo(FrequencyStatus o) {
            if (this.addTime == o.getAddTime())
                return 0;

            return this.addTime > o.getAddTime() ? 1 : -1;
        }
    }
}
