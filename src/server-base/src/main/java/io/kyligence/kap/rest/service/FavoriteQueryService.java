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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.relnode.OLAPContext;
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

import io.kyligence.kap.cube.model.IndexPlan;
import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;

@Component("favoriteQueryService")
public class FavoriteQueryService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteQueryService.class);

    private Map<String, Integer> ignoreCountMap = Maps.newConcurrentMap();

    private static final String LAST_QUERY_TIME = "last_query_time";
    private static final String SUCCESS_RATE = "success_rate";
    private static final String TOTAL_COUNT = "total_count";
    private static final String AVERAGE_DURATION = "average_duration";

    @Autowired
    @Qualifier("favoriteRuleService")
    FavoriteRuleService favoriteRuleService;

    void markFavoriteAndAccelerate(Set<String> sqlPatterns, String project, String user) {
        List<String> sqlsToAccelerate = Lists.newArrayList();
        List<FavoriteQuery> favoriteQueriesToInsert = Lists.newArrayList();

        for (String sqlPattern : sqlPatterns) {
            FavoriteQuery existFavorite = getFavoriteQueryManager(project).get(sqlPattern);
            if (existFavorite != null && !existFavorite.getStatus().equals(FavoriteQueryStatusEnum.WAITING))
                continue;

            sqlsToAccelerate.add(sqlPattern);

            if (existFavorite == null) {
                FavoriteQuery newFavoriteQuery = new FavoriteQuery(sqlPattern);
                newFavoriteQuery.setChannel(FavoriteQuery.CHANNEL_FROM_WHITE_LIST);
                favoriteQueriesToInsert.add(newFavoriteQuery);
            }
        }

        getFavoriteQueryManager(project).create(favoriteQueriesToInsert);
        // accelerate sqls right now
        if (!sqlsToAccelerate.isEmpty())
            handleAccelerate(project, sqlsToAccelerate, user);
    }

    @Transaction(project = 0)
    public void manualFavorite(String project, FavoriteRequest request) throws IOException {
        Preconditions.checkArgument(request.getProject() != null && StringUtils.isNotEmpty(request.getProject()));
        if (QueryHistory.QUERY_HISTORY_FAILED.equals(request.getQueryStatus()))
            return;

        String sqlPattern = request.getSqlPattern();
        Set<String> sqlPatterns = new HashSet<>();
        sqlPatterns.add(sqlPattern);
        favoriteRuleService.appendSqlToWhitelist(request.getSql(), sqlPattern.hashCode(), request.getProject());
        val user = getUsername();
        markFavoriteAndAccelerate(sqlPatterns, project, user);
    }

    public List<FavoriteQuery> filterAndSortFavoriteQueries(String project, String sortBy, boolean reverse, List<String> status) {
        List<FavoriteQuery> favoriteQueries = getFavoriteQueryManager(project).getAll();
        if (CollectionUtils.isNotEmpty(status)) {
            favoriteQueries = favoriteQueries.stream()
                    .filter(favoriteQuery -> status.contains(favoriteQuery.getStatus().toString()))
                    .collect(Collectors.toList());
        }

        return sort(sortBy, reverse, favoriteQueries);
    }

    private List<FavoriteQuery> sort(String sortBy, boolean reverse, List<FavoriteQuery> favoriteQueries) {
        if (sortBy == null) {
            favoriteQueries.sort(Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed());
            return favoriteQueries;
        }

        Comparator comparator;
        switch (sortBy) {
            case LAST_QUERY_TIME:
                comparator = Comparator.comparingLong(FavoriteQuery::getLastQueryTime);
                break;
            case SUCCESS_RATE:
                comparator = Comparator.comparing(FavoriteQuery::getSuccessRate);
                break;
            case TOTAL_COUNT:
                comparator = Comparator.comparingInt(FavoriteQuery::getTotalCount);
                break;
            case AVERAGE_DURATION:
                comparator = Comparator.comparing(FavoriteQuery::getAverageDuration);
                break;
            default:
                comparator = Comparator.comparingLong(FavoriteQuery::getLastQueryTime).reversed();
                favoriteQueries.sort(comparator);
                return favoriteQueries;
        }

        if (reverse)
            comparator = comparator.reversed();

        favoriteQueries.sort(comparator);
        return favoriteQueries;
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

        smartMaster.selectIndexPlan();
        smartMaster.optimizeIndexPlan();

        for (NSmartContext.NModelContext modelContext : modelContexts) {
            List<LayoutEntity> origCuboidLayouts = Lists.newArrayList();
            List<LayoutEntity> targetCuboidLayouts = Lists.newArrayList();

            if (modelContext.getOrigIndexPlan() != null)
                origCuboidLayouts = modelContext.getOrigIndexPlan().getAllLayouts();

            if (modelContext.getTargetIndexPlan() != null)
                targetCuboidLayouts = modelContext.getTargetIndexPlan().getAllLayouts();

            if (!doTwoCuboidLayoutsEqual(origCuboidLayouts, targetCuboidLayouts))
                optimizedModelNum++;
        }

        return optimizedModelNum;
    }

    private boolean doTwoCuboidLayoutsEqual(List<LayoutEntity> origCuboidLayouts,
                                            List<LayoutEntity> targetCuboidLayouts) {
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

        if (unAcceleratedSqls.size() >= projectInstance.getConfig().getFavoriteQueryAccelerateThreshold()
                * ignoreCount) {
            data.put("reach_threshold", true);
            if (!unAcceleratedSqls.isEmpty()) {
                optimizedModelNum = getOptimizedModelNum(project,
                        unAcceleratedSqls.toArray(new String[unAcceleratedSqls.size()]));
            }
        }

        data.put("optimized_model_num", optimizedModelNum);

        return data;
    }

    private List<String> getUnAcceleratedSqlPattern(String project) {
        return getFavoriteQueryManager(project).getUnAcceleratedSqlPattern();
    }

    @Transaction(project = 0)
    public void acceptAccelerate(String project, int accelerateSize) {
        List<String> unAcceleratedSqlPattern = getUnAcceleratedSqlPattern(project);
        if (accelerateSize > unAcceleratedSqlPattern.size()) {
            throw new IllegalArgumentException(
                    String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), accelerateSize));
        }

        accelerate(unAcceleratedSqlPattern.subList(0, accelerateSize), project, getConfig());

        if (ignoreCountMap.containsKey(project))
            ignoreCountMap.put(project, 1);
    }

    public static void accelerate(List<String> unAcceleratedSqlPatterns, String project, KylinConfig config) {
        List<String> sqlPatterns = Lists.newArrayList();
        int batchAccelerateSize = config.getFavoriteAccelerateBatchSize();
        int count = 1;

        for (String sqlPattern : unAcceleratedSqlPatterns) {
            sqlPatterns.add(sqlPattern);

            if (count % batchAccelerateSize == 0) {
                handleAccelerate(project, sqlPatterns, getUsername());
                sqlPatterns.clear();
            }

            count++;
        }

        if (!sqlPatterns.isEmpty()) {
            handleAccelerate(project, sqlPatterns, getUsername());
        }
    }

    public void ignoreAccelerate(String project) {
        int ignoreCount = ignoreCountMap.get(project);
        ignoreCount++;
        ignoreCountMap.put(project, ignoreCount);
    }

    Map<String, Integer> getIgnoreCountMap() {
        return ignoreCountMap;
    }

    static void handleAccelerate(String project, List<String> sqlList, String user) {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (CollectionUtils.isEmpty(sqlList)) {
            return;
        } else {
            // need parse sql
            NSmartMaster master = new NSmartMaster(kylinConfig, project, sqlList.toArray(new String[0]));

            master.runAllAndForContext(smartContext -> {
                if (CollectionUtils.isEmpty(smartContext.getModelContexts())) {
                    return;
                }
                Map<String, AccelerateInfo> blockedSqlInfo = getBlockedSqlInfo(master.getContext());
                EventManager eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {

                    List<String> sqls = getRelatedSqlsFromModelContext(modelContext, blockedSqlInfo);

                    IndexPlan origIndexPlan = modelContext.getOrigIndexPlan();
                    IndexPlan targetIndexPlan = modelContext.getTargetIndexPlan();
                    Pair<List<Long>, List<Long>> updatedLayoutsPair = calcUpdatedLayoutIds(origIndexPlan,
                            targetIndexPlan);
                    List<Long> addedLayoutIds = updatedLayoutsPair.getFirst();
                    if (CollectionUtils.isNotEmpty(addedLayoutIds)) {
                        AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
                        addCuboidEvent.setModelId(targetIndexPlan.getUuid());
                        addCuboidEvent.setSqlPatterns(sqls);
                        addCuboidEvent.setOwner(user);
                        addCuboidEvent.setJobId(UUID.randomUUID().toString());
                        eventManager.post(addCuboidEvent);

                        PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
                        postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
                        postAddCuboidEvent.setModelId(targetIndexPlan.getUuid());
                        postAddCuboidEvent.setOwner(user);
                        postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
                        postAddCuboidEvent.setSqlPatterns(sqls);

                        eventManager.post(postAddCuboidEvent);

                        updateFavoriteQueryStatus(sqls, project, FavoriteQueryStatusEnum.ACCELERATING);
                    } else {
                        updateFavoriteQueryStatus(sqls, project, FavoriteQueryStatusEnum.FULLY_ACCELERATED);
                    }
                }

                if (blockedSqlInfo.size() > 0) {
                    updateBlockedSqlStatus(blockedSqlInfo, KylinConfig.getInstanceFromEnv(), project);
                }
            });
        }
    }

    private static Map<String, AccelerateInfo> getBlockedSqlInfo(NSmartContext context) {
        Map<String, AccelerateInfo> blockedSqlInfo = Maps.newHashMap();
        if (context == null) {
            return blockedSqlInfo;
        }
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        if (accelerateInfoMap == null || accelerateInfoMap.size() == 0) {
            return blockedSqlInfo;
        }
        for (Map.Entry<String, AccelerateInfo> accelerateInfoEntry : accelerateInfoMap.entrySet()) {
            AccelerateInfo accelerateInfo = accelerateInfoEntry.getValue();
            if (accelerateInfo.isBlocked()) {
                blockedSqlInfo.put(accelerateInfoEntry.getKey(), accelerateInfo);
            }
        }
        return blockedSqlInfo;
    }

    private static int BLOCKING_CAUSE_MAX_LENGTH = 500;

    private static void updateBlockedSqlStatus(Map<String, AccelerateInfo> blockedSqlInfo, KylinConfig kylinConfig,
            String project) {
        if (blockedSqlInfo == null || blockedSqlInfo.size() == 0) {
            return;
        }
        FavoriteQuery favoriteQuery;
        val fqMgr = FavoriteQueryManager.getInstance(kylinConfig, project);
        for (Map.Entry<String, AccelerateInfo> accelerateInfoEntry : blockedSqlInfo.entrySet()) {
            String sqlPattern = accelerateInfoEntry.getKey();
            favoriteQuery = new FavoriteQuery(sqlPattern);
            favoriteQuery.setStatus(FavoriteQueryStatusEnum.BLOCKED);
            Throwable blockingCause = accelerateInfoEntry.getValue().getBlockingCause();
            if (blockingCause != null) {
                String blockingCauseStr = blockingCause.toString();
                if (blockingCauseStr.length() > BLOCKING_CAUSE_MAX_LENGTH) {
                    blockingCauseStr = blockingCauseStr.substring(0, BLOCKING_CAUSE_MAX_LENGTH - 1);
                }
                favoriteQuery.setComment(blockingCauseStr);
            }
            fqMgr.updateStatus(sqlPattern, FavoriteQueryStatusEnum.BLOCKED, favoriteQuery.getComment());
        }
    }

    private static void updateFavoriteQueryStatus(List<String> sqlPatterns, String project,
            FavoriteQueryStatusEnum status) {
        val favoriteQueryJDBCDao = FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (String sqlPattern : sqlPatterns) {
            favoriteQueryJDBCDao.updateStatus(sqlPattern, status, null);
        }
    }

    private static List<String> getRelatedSqlsFromModelContext(NSmartContext.NModelContext modelContext,
            Map<String, AccelerateInfo> blockedSqlInfo) {
        List<String> sqls = Lists.newArrayList();
        if (modelContext == null) {
            return sqls;
        }
        ModelTree modelTree = modelContext.getModelTree();
        if (modelTree == null) {
            return sqls;
        }
        Collection<OLAPContext> olapContexts = modelTree.getOlapContexts();
        if (CollectionUtils.isEmpty(olapContexts)) {
            return sqls;
        }
        Iterator<OLAPContext> iterator = olapContexts.iterator();
        while (iterator.hasNext()) {
            String sql = iterator.next().sql;
            if (!blockedSqlInfo.containsKey(sql)) {
                sqls.add(sql);
            }
        }

        return sqls;
    }

    private static Pair<List<Long>, List<Long>> calcUpdatedLayoutIds(IndexPlan origIndexPlan, IndexPlan targetIndexPlan) {
        Pair<List<Long>, List<Long>> pair = new Pair<>();
        List<Long> currentLayoutIds = new ArrayList<>();
        List<Long> toBeLayoutIds = new ArrayList<>();

        List<Long> addedLayoutIds = new ArrayList<>();
        List<Long> removedLayoutIds = new ArrayList<>();
        pair.setFirst(addedLayoutIds);
        pair.setSecond(removedLayoutIds);

        if (origIndexPlan == null && targetIndexPlan == null) {
            return pair;
        }

        currentLayoutIds.addAll(getLayoutIds(origIndexPlan));
        toBeLayoutIds.addAll(getLayoutIds(targetIndexPlan));

        addedLayoutIds.addAll(currentLayoutIds);
        addedLayoutIds.addAll(toBeLayoutIds);
        addedLayoutIds.removeAll(currentLayoutIds);

        removedLayoutIds.addAll(currentLayoutIds);
        removedLayoutIds.addAll(toBeLayoutIds);
        removedLayoutIds.removeAll(toBeLayoutIds);

        return pair;
    }

    private static List<Long> getLayoutIds(IndexPlan indexPlan) {
        List<Long> layoutIds = Lists.newArrayList();
        if (indexPlan == null) {
            return layoutIds;
        }
        List<LayoutEntity> layoutList = indexPlan.getAllLayouts();
        if (CollectionUtils.isEmpty(layoutList)) {
            return layoutIds;
        }

        for (LayoutEntity layout : layoutList) {
            layoutIds.add(layout.getId());
        }
        return layoutIds;
    }

}
