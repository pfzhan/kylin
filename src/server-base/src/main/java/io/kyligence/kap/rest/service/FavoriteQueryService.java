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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.scheduler.FavoriteQueryListNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.favorite.CheckAccelerateSqlListResult;
import io.kyligence.kap.metadata.favorite.CreateFavoriteQueryResult;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.rest.rate.EnableRateLimit;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;
import lombok.var;

@Component("favoriteQueryService")
public class FavoriteQueryService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteQueryService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    private Map<String, Integer> ignoreCountMap = Maps.newConcurrentMap();

    private static final String LAST_QUERY_TIME = "last_query_time";
    private static final String TOTAL_COUNT = "total_count";
    private static final String AVERAGE_DURATION = "average_duration";

    private static final String CAN_BE_ACCELERATED = "can_be_accelerated";
    private static final String WAITING_TAB = "waiting";
    private static final String NOT_ACCELERATED_TAB = "not_accelerated";
    private static final String ACCELERATED_TAB = "accelerated";
    private static final String NOT_SUPPORTED_SQL = "not_supported_sql";

    private static final String IMPORTED = "imported";
    private static final String BLACKLIST = "blacklist";

    private static final String JOB_LIST = "job_list";
    private static final String SQL_ERROR = "sql_error";

    @Transaction(project = 0)
    public CreateFavoriteQueryResult createFavoriteQuery0(String project, FavoriteRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        Map<String, Integer> result = Maps.newHashMap();
        result.put(BLACKLIST, 0);
        result.put(WAITING_TAB, 0);
        result.put(NOT_ACCELERATED_TAB, 0);
        result.put(ACCELERATED_TAB, 0);
        result.put(NOT_SUPPORTED_SQL, 0);

        int importedSqlSize = 0;

        Set<FavoriteQuery> favoriteQueries = new HashSet<>();
        val fqManager = getFavoriteQueryManager(project);
        val blacklistSqls = getFavoriteRuleManager(project).getBlacklistSqls();

        for (String sql : request.getSqls()) {
            if (!QueryUtil.isSelectStatement(sql)) {
                result.computeIfPresent(NOT_SUPPORTED_SQL, (k, v) -> v + 1);
                continue;
            }

            String sqlPattern = QueryPatternUtil.normalizeSQLPattern(sql);

            if (blacklistSqls.contains(sqlPattern)) {
                result.computeIfPresent(BLACKLIST, (k, v) -> v + 1);
                continue;
            }

            val fq = fqManager.get(sqlPattern);
            if (fq != null) {
                if (fq.isInWaitingList()) {
                    result.computeIfPresent(WAITING_TAB, (k, v) -> v + 1);
                }

                if (fq.isNotAccelerated()) {
                    result.computeIfPresent(NOT_ACCELERATED_TAB, (k, v) -> v + 1);
                }

                if (fq.isAccelerated()) {
                    result.computeIfPresent(ACCELERATED_TAB, (k, v) -> v + 1);
                }

                continue;
            }

            FavoriteQuery newFq = new FavoriteQuery(sqlPattern);
            newFq.setChannel(FavoriteQuery.CHANNEL_FROM_IMPORTED);
            favoriteQueries.add(newFq);
            importedSqlSize++;
        }

        fqManager.createWithoutCheck(favoriteQueries);
        result.put(IMPORTED, importedSqlSize);

        return new CreateFavoriteQueryResult(result, favoriteQueries);
    }

    @Transaction(project = 0)
    public Map<String, Integer> createFavoriteQuery(String project, FavoriteRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        return createFavoriteQuery0(project, request).getStatusMap();
    }

    public List<FavoriteQuery> filterAndSortFavoriteQueries(String project, String sortBy, boolean reverse,
            List<String> status) {
        aclEvaluate.checkProjectWritePermission(project);
        // trigger favorite scheduler to fetch latest query histories
        SchedulerEventBusFactory.getInstance(KylinConfig.getInstanceFromEnv())
                .postWithLimit(new FavoriteQueryListNotifier(project));

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

    public Map<String, Integer> getFQSizeInDifferentStatus(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        int canBeAcceleratedSize = 0;
        int waitingSize = 0;
        int notAcceleratedSize = 0;
        int acceleratedSize = 0;

        for (val fq : getFavoriteQueryManager(project).getAll()) {
            if (fq.getStatus().equals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED)
                    || fq.getStatus().equals(FavoriteQueryStatusEnum.PENDING)) {
                canBeAcceleratedSize++;
            }

            if (fq.isInWaitingList())
                waitingSize++;

            if (fq.isNotAccelerated())
                notAcceleratedSize++;

            if (fq.isAccelerated())
                acceleratedSize++;
        }

        Map<String, Integer> result = Maps.newHashMap();
        result.put(CAN_BE_ACCELERATED, canBeAcceleratedSize);
        result.put(WAITING_TAB, waitingSize);
        result.put(NOT_ACCELERATED_TAB, notAcceleratedSize);
        result.put(ACCELERATED_TAB, acceleratedSize);

        return result;
    }

    // only used for test
    public int getOptimizedModelNumForTest(String project, String[] sqls) {
        return getOptimizedModelNum(project, sqls);
    }

    private int getOptimizedModelNum(String project, String[] sqls) {
        int optimizedModelNum = 0;
        NSmartMaster smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), project, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.getContext().setSkipEvaluateCC(true);
        smartMaster.optimizeModel();
        List<NSmartContext.NModelContext> modelContexts = Lists.newArrayList();

        for (NSmartContext.NModelContext modelContext : smartMaster.getContext().getModelContexts()) {
            // case in manual maintain type project and no model is selected
            if (modelContext.getOriginModel() == null && modelContext.getTargetModel() == null)
                continue;

            if ((modelContext.getOriginModel() == null && modelContext.getTargetModel() != null)
                    || !modelContext.getOriginModel().equals(modelContext.getTargetModel())) {
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

            if (modelContext.getOriginIndexPlan() != null)
                origCuboidLayouts = modelContext.getOriginIndexPlan().getAllLayouts();

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
        aclEvaluate.checkProjectWritePermission(project);
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        Map<String, Object> data = Maps.newHashMap();
        List<String> waitingAcceleratedSqls = getWaitingAcceleratingSqlPattern(project);
        int optimizedModelNum = 0;

        data.put("size", waitingAcceleratedSqls.size());
        data.put("reach_threshold", false);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        int ignoreCount = 1;
        if (ignoreCountMap.containsKey(project))
            ignoreCount = ignoreCountMap.get(project);

        if (waitingAcceleratedSqls.size() >= projectInstance.getConfig().getFavoriteQueryAccelerateThreshold()
                * ignoreCount) {
            data.put("reach_threshold", true);
            if (!waitingAcceleratedSqls.isEmpty()) {
                optimizedModelNum = getOptimizedModelNum(project, waitingAcceleratedSqls.toArray(new String[0]));
            }
        }

        data.put("optimized_model_num", optimizedModelNum);

        return data;
    }

    private List<String> getAccelerableSqlPattern(String project) {
        return getFavoriteQueryManager(project).getAccelerableSqlPattern();
    }

    private List<String> getWaitingAcceleratingSqlPattern(String project) {
        return getFavoriteQueryManager(project).getToBeAcceleratedSqlPattern();
    }

    @VisibleForTesting
    public CheckAccelerateSqlListResult checkAccelerateSqlList(String project, final List<String> sqls) {
        List<String> validateSqls = Lists.newArrayList();
        List<String> errorSqls = Lists.newArrayList();
        favoriteRuleService.batchSqlValidate(sqls, project).forEach((sql, validateResult) -> {
            if (validateResult.isCapable()) {
                validateSqls.add(sql);
            } else {
                errorSqls.add(sql);
            }
        });

        return new CheckAccelerateSqlListResult(validateSqls, errorSqls);
    }

    @Transaction(project = 0)
    public Map<String, List<String>> acceptAccelerate(String project, final List<String> sqls) {
        aclEvaluate.checkProjectWritePermission(project);
        Preconditions.checkNotNull(sqls);
        Map<String, List<String>> result = Maps.newHashMap();
        result.put(SQL_ERROR, Lists.newArrayList());
        result.put(JOB_LIST, Lists.newArrayList());

        if (CollectionUtils.isEmpty(sqls)) {
            return result;
        }

        val checkResult = checkAccelerateSqlList(project, sqls);
        result.get(SQL_ERROR).addAll(checkResult.getErrorSqls());

        FavoriteRequest request = new FavoriteRequest();
        request.setProject(project);
        request.setSqls(checkResult.getValidateSqls());
        val createResult = createFavoriteQuery0(project, request);
        List<String> importedSql = createResult.getFavoriteQuerySet().stream().map(FavoriteQuery::getSqlPattern)
                .collect(Collectors.toList());

        result.get(JOB_LIST).addAll(accelerate(importedSql, project, getConfig()));

        if (ignoreCountMap.containsKey(project))
            ignoreCountMap.put(project, 1);

        return result;
    }

    @Transaction(project = 0)
    public void acceptAccelerate(String project, int accelerateSize) {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> unAcceleratedSqlPattern = getAccelerableSqlPattern(project);
        if (accelerateSize > unAcceleratedSqlPattern.size()) {
            throw new IllegalArgumentException(
                    String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), accelerateSize));
        }

        if (accelerateSize < unAcceleratedSqlPattern.size()) {
            accelerateSize = unAcceleratedSqlPattern.size();
        }

        accelerate(unAcceleratedSqlPattern.subList(0, accelerateSize), project, getConfig());

        if (ignoreCountMap.containsKey(project))
            ignoreCountMap.put(project, 1);
    }

    /**
     * auto recommendation for semi-auto-mode
     */
    public void generateRecommendation() {
        String oldThreadName = Thread.currentThread().getName();
        EpochManager epochManager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());
        try {
            Thread.currentThread().setName("AutoRecommendation");
            getProjectManager().listAllProjects().stream() //
                    .filter(ProjectInstance::isSemiAutoMode)
                    .filter(p -> epochManager.checkEpochOwner(p.getName()))
                    .forEach(projectInstance -> {
                        try {
                            String projectName = projectInstance.getName();
                            val config = KylinConfig.getInstanceFromEnv();
                            accelerate(getAccelerableSqlPattern(projectName), projectName, config);
                        } catch (Exception e) {
                            logger.error("generate recommendations<" + projectInstance.getName() + "> failed", e);
                        }
                    });
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }
    }

    @Transaction(project = 1)
    private List<String> accelerate(List<String> unAcceleratedSqlPatterns, String project, KylinConfig config) {
        List<String> jobList = Lists.newArrayList();

        int batchAccelerateSize = config.getFavoriteAccelerateBatchSize();
        ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(project);
        Lists.partition(unAcceleratedSqlPatterns, batchAccelerateSize).forEach(oneBatchPatterns -> {
            if (projectInstance.isSemiAutoMode()) {
                generateSuggestions(project, oneBatchPatterns);
            } else {
                jobList.addAll(handleAcceleration(project, oneBatchPatterns, getUsername()));
            }
        });
        return jobList;
    }

    public void ignoreAccelerate(String project, int ignoreSize) {
        aclEvaluate.checkProjectWritePermission(project);
        var projectInstance = getProjectManager().getProject(project);
        int threshold = projectInstance.getConfig().getFavoriteQueryAccelerateThreshold();

        int ignoreCount = ignoreSize / threshold + 1;
        ignoreCountMap.put(project, ignoreCount);
    }

    private void generateSuggestions(String project, List<String> sqlList) {
        if (CollectionUtils.isEmpty(sqlList)) {
            return;
        }
        long startTime = System.currentTimeMillis();
        logger.info("Semi-Auto-Mode project:{} generate suggestions by sqlList size: {}", project, sqlList.size());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Set<String> sqlSet = Sets.newHashSet(sqlList);
        NSmartMaster master = new NSmartMaster(kylinConfig, project, sqlList.toArray(new String[0]));
        master.runOptRecommendation(smartContext -> {
            Map<String, AccelerateInfo> notAccelerated = getNotAcceleratedSqlInfo(master.getContext());
            if (!notAccelerated.isEmpty()) {
                sqlSet.removeAll(notAccelerated.keySet());
                updateNotAcceleratedSqlStatus(notAccelerated, KylinConfig.getInstanceFromEnv(), project);
            } // case of sqls with constants
            if (CollectionUtils.isEmpty(smartContext.getModelContexts())) {
                updateFavoriteQueryStatus(sqlSet, project, FavoriteQueryStatusEnum.ACCELERATED);
                return;
            }
            for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {
                val sqls = getRelatedSqlsFromModelContext(modelContext, notAccelerated);
                sqlSet.removeAll(sqls);
                if (CollectionUtils.isEmpty(sqls)) {
                    continue;
                }
                updateFavoriteQueryStatus(sqls, project, FavoriteQueryStatusEnum.ACCELERATED);
            }
            updateFavoriteQueryStatus(sqlSet, project, FavoriteQueryStatusEnum.ACCELERATED);
        });
        logger.info("Semi-Auto-Mode project:{} generate suggestions cost {}ms", project,
                System.currentTimeMillis() - startTime);
    }

    private List<String> handleAcceleration(String project, List<String> sqlList, String user) {
        List<String> jobList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(sqlList))
            return jobList;

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Set<String> sqlSet = Sets.newHashSet(sqlList);

        // do auto-modeling
        NSmartMaster master = new NSmartMaster(kylinConfig, project, sqlList.toArray(new String[0]));
        master.runAllAndForContext(smartContext -> {
            // handle blocked sql patterns
            Map<String, AccelerateInfo> notAccelerated = getNotAcceleratedSqlInfo(master.getContext());
            if (!notAccelerated.isEmpty()) {
                sqlSet.removeAll(notAccelerated.keySet());
                updateNotAcceleratedSqlStatus(notAccelerated, KylinConfig.getInstanceFromEnv(), project);
            } // case of sqls with constants
            if (CollectionUtils.isEmpty(smartContext.getModelContexts())) {
                updateFavoriteQueryStatus(sqlSet, project, FavoriteQueryStatusEnum.ACCELERATED);
                return;
            }

            EventManager eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {

                val sqls = getRelatedSqlsFromModelContext(modelContext, notAccelerated);
                sqlSet.removeAll(sqls);
                IndexPlan targetIndexPlan = modelContext.getTargetIndexPlan();

                if (CollectionUtils.isEmpty(sqls)) {
                    continue;
                }

                jobList.add(eventManager.postAddCuboidEvents(targetIndexPlan.getUuid(), user));

                updateFavoriteQueryStatus(sqls, project, FavoriteQueryStatusEnum.ACCELERATING);
            }
            updateFavoriteQueryStatus(sqlSet, project, FavoriteQueryStatusEnum.ACCELERATED);
        });
        return jobList;
    }

    // including pending and failed fqs
    private Map<String, AccelerateInfo> getNotAcceleratedSqlInfo(NSmartContext context) {
        Map<String, AccelerateInfo> notAcceleratedSqlInfo = Maps.newHashMap();
        if (context == null) {
            return notAcceleratedSqlInfo;
        }
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        if (MapUtils.isEmpty(accelerateInfoMap)) {
            return notAcceleratedSqlInfo;
        }
        for (Map.Entry<String, AccelerateInfo> accelerateInfoEntry : accelerateInfoMap.entrySet()) {
            AccelerateInfo accelerateInfo = accelerateInfoEntry.getValue();
            if (accelerateInfo.isNotSucceed()) {
                notAcceleratedSqlInfo.put(accelerateInfoEntry.getKey(), accelerateInfo);
            }
        }
        return notAcceleratedSqlInfo;
    }

    private static final int BLOCKING_CAUSE_MAX_LENGTH = 500;

    // only for test
    public void updateNotAcceleratedSqlStatusForTest(Map<String, AccelerateInfo> notAcceleratedSqlInfo,
            KylinConfig kylinConfig, String project) {
        updateNotAcceleratedSqlStatus(notAcceleratedSqlInfo, kylinConfig, project);
    }

    private void updateNotAcceleratedSqlStatus(Map<String, AccelerateInfo> notAcceleratedSqlInfo,
            KylinConfig kylinConfig, String project) {
        if (MapUtils.isEmpty(notAcceleratedSqlInfo)) {
            return;
        }
        val fqMgr = FavoriteQueryManager.getInstance(kylinConfig, project);
        for (Map.Entry<String, AccelerateInfo> accelerateInfoEntry : notAcceleratedSqlInfo.entrySet()) {
            String sqlPattern = accelerateInfoEntry.getKey();

            val accelerateInfo = accelerateInfoEntry.getValue();
            if (accelerateInfo.isPending()) {
                fqMgr.updateStatus(sqlPattern, FavoriteQueryStatusEnum.PENDING, accelerateInfo.getPendingMsg());
                continue;
            }

            Throwable blockingCause = accelerateInfoEntry.getValue().getFailedCause();
            if (blockingCause != null) {
                String blockingCauseStr = blockingCause.getMessage();
                if (blockingCauseStr != null && blockingCauseStr.length() > BLOCKING_CAUSE_MAX_LENGTH) {
                    blockingCauseStr = blockingCauseStr.substring(0, BLOCKING_CAUSE_MAX_LENGTH - 1);
                }
                fqMgr.updateStatus(sqlPattern, FavoriteQueryStatusEnum.FAILED, blockingCauseStr);
            }
        }
    }

    private void updateFavoriteQueryStatus(Set<String> sqlPatterns, String project, FavoriteQueryStatusEnum status) {
        if (CollectionUtils.isEmpty(sqlPatterns))
            return;

        val favoriteQueryManager = FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (String sqlPattern : sqlPatterns) {
            favoriteQueryManager.updateStatus(sqlPattern, status, null);
        }
    }

    private Set<String> getRelatedSqlsFromModelContext(NSmartContext.NModelContext modelContext,
            Map<String, AccelerateInfo> blockedSqlInfo) {
        Set<String> sqls = Sets.newHashSet();
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

    @Async
    @EnableRateLimit
    public void asyncAdjustFavoriteQuery(String projectName) {
        String oldThreadName = Thread.currentThread().getName();

        try {
            Thread.currentThread().setName("FavoriteQueryAdjustWorker");
            adjustFQForProject(projectName);
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }
    }

    public void adjustFalseAcceleratedFQ() {
        String oldThreadName = Thread.currentThread().getName();

        try {
            Thread.currentThread().setName("FavoriteQueryAdjustWorker");

            List<ProjectInstance> nonExpertProjects = getProjectManager().listAllProjects().stream()
                    .filter(projectInstance -> !projectInstance.isExpertMode()) //
                    .collect(Collectors.toList());
            for (ProjectInstance project : nonExpertProjects) {
                if (!KylinConfig.getInstanceFromEnv().isUTEnv() && !EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).checkEpochOwner(project.getName()))
                    continue;
                try {
                    adjustFQForProject(project.getName());
                } catch (Exception e) {
                    logger.warn("adjust false accelerated favorite query<" + project.getName() + "> failed", e);
                }
            }
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }
    }

    private void adjustFQForProject(String project) {
        NMetricsGroup.counterInc(NMetricsName.FQ_ADJUST_INVOKED, NMetricsCategory.PROJECT, project);
        logger.trace("Start checking favorite query accelerate status adjustment for project {}.", project);
        long startTime = System.currentTimeMillis();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<String> sqlPatterns = FavoriteQueryManager.getInstance(config, project).getAcceleratedSqlPattern();
        // split sqlPatterns into batches to avoid
        int batchOffset = 0;
        int batchSize = config.getAutoCheckAccStatusBatchSize();
        Preconditions.checkArgument(batchSize > 0, "Illegal batch size: " + batchSize
                + ". Please check config: kylin.favorite.auto-check-accelerate-batch-size");
        int sqlSize = sqlPatterns.size();
        while (batchOffset < sqlSize) {
            int batchStart = batchOffset;
            batchOffset = Math.min(batchOffset + batchSize, sqlSize);
            String[] sqls = sqlPatterns.subList(batchStart, batchOffset).toArray(new String[0]);
            checkAccelerateStatus(project, sqls);
        }
        long duration = System.currentTimeMillis() - startTime;
        logger.trace("End favorite query adjustment. Processed {} queries and took {}ms for project {}", sqlSize,
                duration, project);
        NMetricsGroup.counterInc(NMetricsName.FQ_ADJUST_INVOKED_DURATION, NMetricsCategory.PROJECT, project, duration);
    }

    private void checkAccelerateStatus(String project, String[] sqls) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            FavoriteQueryManager manager = FavoriteQueryManager.getInstance(config, project);
            NSmartMaster master = new NSmartMaster(config, project, sqls);
            master.selectAndOptimize();
            String[] toUpdateSqls = Arrays.stream(sqls).filter(sql -> {
                // only unmatched to handle
                FavoriteQuery fq = manager.get(sql);
                AccelerateInfo accInfo = master.getContext().getAccelerateInfoMap().get(sql);
                return !matchAccelerateInfo(fq, accInfo);
            }).toArray(String[]::new);

            FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project);
            Arrays.stream(toUpdateSqls).forEach(sql -> favoriteQueryManager.rollBackToInitialStatus(sql,
                    "This query is not fully accelerated, move status to WAITING"));
            logger.info("There are {} favorite queries not fully accelerated, changed status to WAITING",
                    toUpdateSqls.length);
            return null;
        }, project);
    }

    private boolean matchAccelerateInfo(FavoriteQuery fq, AccelerateInfo accInfo) {
        if (fq == null) {
            return false;
        }
        List<FavoriteQueryRealization> favoriteQueryRealizations = fq.getRealizations();

        if (accInfo == null) {
            return false;
        }
        Set<AccelerateInfo.QueryLayoutRelation> suggestedQueryRealizations = accInfo.getRelatedLayouts();
        if (favoriteQueryRealizations.size() != suggestedQueryRealizations.size()) {
            return false;
        }
        List<String> fqrInfo = favoriteQueryRealizations.stream().map(real -> new StringBuilder(real.getModelId())
                .append('_').append(real.getSemanticVersion()).append('_').append(real.getLayoutId()).toString())
                .sorted().collect(Collectors.toList());
        List<String> sqrInfo = suggestedQueryRealizations.stream().map(real -> new StringBuilder(real.getModelId())
                .append('_').append(real.getSemanticVersion()).append('_').append(real.getLayoutId()).toString())
                .sorted().collect(Collectors.toList());
        return fqrInfo.equals(sqrInfo);
    }
}
