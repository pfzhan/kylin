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
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import io.kylingence.kap.event.handle.AddCuboidHandler;
import io.kylingence.kap.event.handle.ModelUpdateHandler;
import io.kylingence.kap.event.handle.RemoveCuboidHandler;
import io.kylingence.kap.event.model.ModelUpdateEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.InternalErrorException;
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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component("favoriteQueryService")
public class FavoriteQueryService extends BasicService {
    AutoMarkFavorite autoMarkFavorite = new AutoMarkFavorite();

    @Autowired
    @Qualifier("queryHistoryService")
    QueryHistoryService queryHistoryService;

    @PostConstruct
    void init() throws SchedulerException {
        new ModelUpdateHandler();
        new AddCuboidHandler();
        new RemoveCuboidHandler();
        autoMarkFavorite.start();
    }

    public List<FavoriteQuery> favorite(String project, List<String> queryUuids) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        List<QueryHistory> queryHistories = Lists.newArrayList();
        QueryHistoryManager manager = getQueryHistoryManager(project);
        for (String query : queryUuids) {
            queryHistories.add(manager.findQueryHistory(query));
        }

        return internalFavorite(project, queryHistories);
    }

    private List<FavoriteQuery> internalFavorite(String project, List<QueryHistory> queryHistories) throws IOException {
        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
        Map<String, List<QueryHistory>> sqlMap = Maps.newHashMap();
        FavoriteQueryManager favoriteQueryManager = getFavoriteQueryManager(project);

        // get distinct sqls
        for (QueryHistory queryHistory : queryHistories) {
            if (queryHistory == null) {
                throw new NotFoundException(MsgPicker.getMsg().getQUERY_HISTORY_NOT_FOUND());
            }

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
                    throw new IllegalStateException(MsgPicker.getMsg().getQUERY_HISTORY_IS_FAVORITED());
                }

                queryHistory.setFavorite(favoriteQuery.getUuid());
                queryHistory.setAccelerateStatus(favoriteQuery.getStatus().toString());
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
                        String.format(MsgPicker.getMsg().getFAVORITE_QUERY_NOT_FOUND(), favoriteQuery.getSql()));
            }

            favoriteQueryManager.unFavor(favoriteQuery);
            sqls.put(favoriteQuery.getSql(), favoriteUuid);

            List<QueryHistory> queryHistories = queryHistoryManager.findQueryHistoryByFavorite(favoriteUuid);

            for (QueryHistory queryHistory : queryHistories) {
                if (!queryHistory.isFavorite()) {
                    throw new IllegalStateException(MsgPicker.getMsg().getQUERY_HISTORY_IS_NOT_FAVORITED());
                }

                queryHistory.setFavorite(null);
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

        return favoriteQueries;
    }

    private List<FavoriteQuery> getUnAcceleratedQueries(String project) {
        List<FavoriteQuery> result = Lists.newArrayList();

        for (FavoriteQuery favoriteQuery : getFavoriteQueryManager(project).getAll()) {
            if (favoriteQuery.getStatus() == FavoriteQueryStatusEnum.WAITING)
                result.add(favoriteQuery);
        }

        return result;
    }

    public HashMap<String, Object> isTimeToAccelerate(String project) {
        HashMap<String, Object> data = Maps.newHashMap();
        List<FavoriteQuery> unAcceleratedQueries = getUnAcceleratedQueries(project);

        data.put("size", unAcceleratedQueries.size());
        data.put("unAccelerated_queries", unAcceleratedQueries);
        data.put("reach_threshold", false);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        if (unAcceleratedQueries.size() >= projectInstance.getConfig().getFavoriteQueryAccelerateThreshold())
            data.put("reach_threshold", true);

        return data;
    }

    public void acceptAccelerate(String project) throws Exception {
        Map<String, String> sqls = Maps.newHashMap();
        for (final FavoriteQuery favoriteQuery : getUnAcceleratedQueries(project)) {
            sqls.put(favoriteQuery.getSql(), favoriteQuery.getUuid());

            favoriteQuery.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
            getFavoriteQueryManager(project).update(favoriteQuery);
        }

        post(project, sqls, true);
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

    public void saveQueryFilterRule(String project, List<QueryFilterRule> rules) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        QueryFilterRuleManager manager = getQueryFilterRuleManager(project);
        for (QueryFilterRule rule : rules) {
            manager.save(rule);
        }
    }

    public void deleteQueryFilterRule(String project, List<String> ruleIds) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        QueryFilterRuleManager manager = getQueryFilterRuleManager(project);
        for (String ruleId : ruleIds) {
            QueryFilterRule rule = manager.get(ruleId);
            if (rule != null)
                manager.delete(rule);
            else
                throw new IllegalArgumentException(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), ruleId));
        }
    }

    public List<QueryHistory> getCandidates(String project) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        List<QueryHistory> unFavoriteQueries = getQueryHistoryManager(project).getUnFavoriteQueryHistory();
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
            throw new InternalErrorException(MsgPicker.getMsg().getPROJECT_NOT_FOUND());
        }

        boolean isMarkAutomatic = projectInstance.getConfig().isAutoMarkFavorite();

        LinkedHashMap<String, String> overrideConfig = Maps.newLinkedHashMap(projectInstance.getOverrideKylinProps());
        overrideConfig.put("kylin.favorite.auto-mark", String.valueOf(!isMarkAutomatic));
        getProjectManager().updateProject(projectInstance, projectInstance.getName(), projectInstance.getDescription(),
                overrideConfig);
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
