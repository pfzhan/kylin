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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kylingence.kap.event.model.ModelUpdateEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component("favoriteQueryService")
public class FavoriteQueryService extends BasicService {
    AutoMarkFavorite autoMarkFavorite = new AutoMarkFavorite();
    private Map<String, Integer> ignoreCountMap = Maps.newHashMap();

    @Autowired
    @Qualifier("queryHistoryService")
    QueryHistoryService queryHistoryService;

    @PostConstruct
    void init() {
        autoMarkFavorite.start();
    }

    public List<FavoriteQuery> favorite(String project, List<String> queryUuids) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        List<QueryHistory> queryHistories = Lists.newArrayList();
        QueryHistoryManager manager = getQueryHistoryManager(project);
        for (String queryUuid : queryUuids) {
            QueryHistory queryHistory = manager.findQueryHistory(queryUuid);
            if (queryHistory == null)
                throw new NotFoundException(String.format(MsgPicker.getMsg().getQUERY_HISTORY_NOT_FOUND(), queryUuid));
            queryHistories.add(queryHistory);
        }

        return internalFavorite(project, queryHistories);
    }

    private List<FavoriteQuery> internalFavorite(String project, List<QueryHistory> queryHistories) throws IOException {
        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
        Map<String, List<QueryHistory>> sqlMap = Maps.newHashMap();
        FavoriteQueryManager favoriteQueryManager = getFavoriteQueryManager(project);

        // get distinct sqls
        for (QueryHistory queryHistory : queryHistories) {

            if (!sqlMap.containsKey(queryHistory.getSql())) {
                sqlMap.put(queryHistory.getSql(), Lists.newArrayList(queryHistory));
            } else {
                List<QueryHistory> queries = sqlMap.get(queryHistory.getSql());
                queries.add(queryHistory);
                sqlMap.put(queryHistory.getSql(), queries);
            }
        }

        // mark favorite either from existing favorite query or from new created one
        for (final Map.Entry<String, List<QueryHistory>> entry : sqlMap.entrySet()) {
            FavoriteQuery favoriteQuery = favoriteQueryManager.findFavoriteQueryBySql(entry.getKey());

            if (favoriteQuery == null) {
                favoriteQuery = new FavoriteQuery(entry.getKey());
                favoriteQueryManager.favor(favoriteQuery);
            }

            favoriteQueries.add(favoriteQuery);

            for (QueryHistory queryHistory : entry.getValue()) {
                if (queryHistory.isFavorite()) {
                    throw new IllegalStateException(String.format(MsgPicker.getMsg().getQUERY_HISTORY_IS_FAVORITED(), queryHistory.getUuid()));
                }

                queryHistory.setFavorite(favoriteQuery.getUuid());
                getQueryHistoryManager(project).save(queryHistory);
            }
        }

        return favoriteQueries;
    }

    public void unFavorite(String project, List<String> favorites) throws Exception {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        FavoriteQueryManager favoriteQueryManager = getFavoriteQueryManager(project);
        QueryHistoryManager queryHistoryManager = getQueryHistoryManager(project);

        Map<String, String> sqls = Maps.newHashMap();
        for (final String favoriteUuid : favorites) {
            final FavoriteQuery favoriteQuery = favoriteQueryManager.get(favoriteUuid);

            if (favoriteQuery == null) {
                throw new NotFoundException(
                        String.format(MsgPicker.getMsg().getFAVORITE_QUERY_NOT_FOUND(), favoriteUuid));
            }

            favoriteQueryManager.unFavor(favoriteQuery);
            sqls.put(favoriteQuery.getSql(), favoriteUuid);

            List<QueryHistory> queryHistories = queryHistoryManager.findQueryHistoryByFavorite(favoriteUuid);

            for (QueryHistory queryHistory : queryHistories) {

                queryHistory.setFavorite(null);
                queryHistory.setUnfavorite(true);
                queryHistoryManager.save(queryHistory);
            }
        }

        post(project, sqls, false);

    }

    private boolean isInTheSameDay(Calendar cal1, Calendar cal2) {
        return cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR) && cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR);
    }

    public List<FavoriteQuery> getAllFavoriteQueries(String project) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));

        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
        FavoriteQueryManager favoriteQueryManager = getFavoriteQueryManager(project);
        Calendar now = Calendar.getInstance();
        now.setTime(new Date());
        Calendar queryStartTime = Calendar.getInstance();
        for (FavoriteQuery favoriteQuery : getFavoriteQueryManager(project).getAll()) {
            int frequency = 0;
            int successCount = 0;
            float totalDuration = 0f;
            long lastQueryTime = 0;
            for (QueryHistory queryHistory : getQueryHistoryManager(project).getAllQueryHistories()) {
                queryStartTime.setTime(new Date(queryHistory.getStartTime()));
                if (!queryHistory.getSql().equals(favoriteQuery.getSql()) || !isInTheSameDay(now, queryStartTime))
                    continue;

                frequency ++;
                totalDuration += queryHistory.getLatency();
                if (queryHistory.getQueryStatus().equals(QueryHistoryStatusEnum.SUCCEEDED))
                    successCount++;
                if (queryHistory.getStartTime() > lastQueryTime)
                    lastQueryTime = queryHistory.getStartTime();
            }

            favoriteQuery.setLastQueryTime(lastQueryTime);
            favoriteQuery.setFrequency(frequency);
            if (frequency != 0) {
                favoriteQuery.setSuccessRate((float) successCount / frequency);
                favoriteQuery.setAverageDuration(totalDuration / frequency);
            } else {
                favoriteQuery.setSuccessRate(0);
                favoriteQuery.setAverageDuration(0f);
            }
            favoriteQueryManager.update(favoriteQuery);

            favoriteQueries.add(favoriteQuery);
        }

        Collections.sort(favoriteQueries, Collections.reverseOrder());
        return favoriteQueries;
    }

    List<FavoriteQuery> getUnAcceleratedQueries(String project) {
        List<FavoriteQuery> result = Lists.newArrayList();

        for (FavoriteQuery favoriteQuery : getFavoriteQueryManager(project).getAll()) {
            if (favoriteQuery.getStatus() == FavoriteQueryStatusEnum.WAITING)
                result.add(favoriteQuery);
        }

        return result;
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
            if ((modelContext.getOrigModel() == null && modelContext.getTargetModel() != null) ||
                    !modelContext.getOrigModel().equals(modelContext.getTargetModel())) {
                optimizedModelNum ++;
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
                optimizedModelNum ++;
        }

        return optimizedModelNum;
    }

    private boolean doTwoCubiodLayoutsEqual(List<NCuboidLayout> origCuboidLayouts, List<NCuboidLayout> targetCuboidLayouts) {
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
        List<FavoriteQuery> unAcceleratedQueries = getUnAcceleratedQueries(project);
        int optimizedModelNum = 0;

        if (!unAcceleratedQueries.isEmpty()) {
            String[] sqls = new String[unAcceleratedQueries.size()];
            for (int i = 0; i < unAcceleratedQueries.size(); i++) {
                sqls[i] = unAcceleratedQueries.get(i).getSql();
            }

            optimizedModelNum = getOptimizedModelNum(project, sqls);
        }

        data.put("size", unAcceleratedQueries.size());
        data.put("reach_threshold", false);
        data.put("optimized_model_num", optimizedModelNum);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        int ignoreCount = 1;
        if (ignoreCountMap.containsKey(project))
            ignoreCount = ignoreCountMap.get(project);
        else
            ignoreCountMap.put(project, 1);

        if (unAcceleratedQueries.size() >= projectInstance.getConfig().getFavoriteQueryAccelerateThreshold() * ignoreCount)
            data.put("reach_threshold", true);

        return data;
    }

    public void acceptAccelerate(String project, int accelerateSize) throws Exception {
        Map<String, String> sqls = Maps.newHashMap();
        List<FavoriteQuery> unAcceleratedQueries = getUnAcceleratedQueries(project);
        if (accelerateSize > unAcceleratedQueries.size()) {
            throw new IllegalArgumentException(String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), accelerateSize));
        }
        for (final FavoriteQuery favoriteQuery : unAcceleratedQueries.subList(0, accelerateSize)) {
            sqls.put(favoriteQuery.getSql(), favoriteQuery.getUuid());

            favoriteQuery.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
            getFavoriteQueryManager(project).update(favoriteQuery);
        }

        if (ignoreCountMap.containsKey(project))
            ignoreCountMap.put(project, 1);

        post(project, sqls, true);
    }

    public void ignoreAccelerate(String project) {
        int ignoreCount = ignoreCountMap.get(project);
        ignoreCount ++;
        ignoreCountMap.put(project, ignoreCount);
    }

    Map<String, Integer> getIgnoreCountMap() {
        return ignoreCountMap;
    }

    void post(String project, Map<String, String> sqls, boolean favoriteMark) throws PersistentException {
        ModelUpdateEvent modelUpdateEvent = new ModelUpdateEvent();
        modelUpdateEvent.setFavoriteMark(favoriteMark);
        modelUpdateEvent.setProject(project);
        modelUpdateEvent.setSqlMap(sqls);
        modelUpdateEvent.setApproved(true);
        getEventManager(project).post(modelUpdateEvent);
    }

    public List<QueryFilterRule> getQueryFilterRules(String project) {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        return getQueryFilterRuleManager(project).getAll();
    }

    public void saveQueryFilterRule(String project, QueryFilterRule rule) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        getQueryFilterRuleManager(project).save(rule);
    }

    public void enableQueryFilterRule(String project, String uuid) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        QueryFilterRuleManager manager = getQueryFilterRuleManager(project);
        QueryFilterRule rule = manager.get(uuid);
        if (rule == null)
            throw new NotFoundException(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), uuid));

        boolean isEnabled = rule.isEnabled();
        rule.setEnabled(!isEnabled);
        manager.save(rule);
    }

    public void deleteQueryFilterRule(String project, String ruleId) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        QueryFilterRuleManager manager = getQueryFilterRuleManager(project);
        QueryFilterRule rule = manager.get(ruleId);
        if (rule != null)
            manager.delete(rule);
        else
            throw new IllegalArgumentException(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), ruleId));
    }

    public List<QueryHistory> getCandidates(String project) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        List<QueryHistory> unFavoriteQueries = Lists.newArrayList();
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        if (projectInstance.getConfig().isAutoMarkFavorite())
            unFavoriteQueries = getQueryHistoryManager(project).getUnFavoriteQueryHistoryForAuto();
        else
            unFavoriteQueries = getQueryHistoryManager(project).getUnFavoriteQueryHistoryForManual();
        return queryHistoryService.getQueryHistoriesByRules(getQueryFilterRuleManager(project).getAllEnabled(), unFavoriteQueries);
    }

    public void applyAll(String project) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        QueryFilterRuleManager manager = getQueryFilterRuleManager(project);
        for (QueryFilterRule rule : manager.getAll()) {
            if (!rule.isEnabled()) {
                rule.setEnabled(true);
                manager.save(rule);
            }
        }
    }

    public void markAutomatic(String project) throws IOException {
        ProjectInstance projectInstance = getProjectManager().getProject(project);

        if (projectInstance == null) {
            throw new NotFoundException(String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }

        boolean isMarkAutomatic = projectInstance.getConfig().isAutoMarkFavorite();

        LinkedHashMap<String, String> overrideConfig = Maps.newLinkedHashMap(projectInstance.getOverrideKylinProps());
        overrideConfig.put("kylin.favorite.auto-mark", String.valueOf(!isMarkAutomatic));
        getProjectManager().updateProject(projectInstance, projectInstance.getName(), projectInstance.getDescription(),
                overrideConfig);
    }

    public boolean getMarkAutomatic(String project) {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        return projectInstance.getConfig().isAutoMarkFavorite();
    }

    public class AutoMarkFavorite extends Thread {
        private final Logger logger = LoggerFactory.getLogger(AutoMarkFavorite.class);

        private final NProjectManager projectManager;
        private boolean isAutoMarkFavoriteMode;
        private long detectionInterval;

        public AutoMarkFavorite() {
            super("AutoMarkFavorite");
            this.setDaemon(true);
            this.projectManager = getProjectManager();
        }

        @Override
        public void run() {
            while (true) {
                logger.info("auto mark favorite thread");

                try {
                    for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
                        isAutoMarkFavoriteMode = projectInstance.getConfig().isAutoMarkFavorite();
                        String projectName = projectInstance.getName();
                        if (isAutoMarkFavoriteMode) {
                            internalFavorite(projectName, getCandidates(projectName));
                        }
                    }
                    this.detectionInterval = KylinConfig.getInstanceFromEnv().getAutoMarkFavoriteDetectionInterval() * 1000L;
                } catch (Throwable ex) {
                    logger.error(ex.getLocalizedMessage(), ex);
                }

                try {
                    Thread.sleep(detectionInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // stop detection and exit
                    return;
                }
            }
        }
    }
}
