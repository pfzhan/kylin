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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.guava20.shaded.common.collect.ArrayListMultimap;
import io.kyligence.kap.guava20.shaded.common.collect.ListMultimap;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.LayoutMetric;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("rawRecService")
public class RawRecService {

    private static final String ACCELERATION_INTERRUPT_BY_USER = "Acceleration triggered by user terminate the process of generate recommendation automatically at present.";

    @Autowired
    ProjectService projectService;

    @Autowired
    OptRecService optRecService;

    public void accelerate(String project) {
        projectService.accelerateImmediately(project);
        updateCostsAndTopNCandidates(project);
    }

    public void transferAndSaveRecommendations(AbstractContext proposeContext) {
        if (!(proposeContext instanceof ModelReuseContextOfSemiV2)) {
            return;
        }
        ModelReuseContextOfSemiV2 semiContextV2 = (ModelReuseContextOfSemiV2) proposeContext;
        Map<String, RawRecItem> nonLayoutUniqueFlagRecMap = semiContextV2.getExistingNonLayoutRecItemMap();
        transferAndSaveModelRelatedRecItems(semiContextV2, nonLayoutUniqueFlagRecMap);

        List<RawRecItem> layoutRecItems = transferToLayoutRecItems(semiContextV2, ArrayListMultimap.create(),
                nonLayoutUniqueFlagRecMap);
        if (QueryHistoryTaskScheduler.getInstance(semiContextV2.getProject()).isInterruptByUser()) {
            throw new IllegalStateException(RawRecService.ACCELERATION_INTERRUPT_BY_USER);
        }
        saveLayoutRawRecItems(layoutRecItems, semiContextV2.getProject());
    }

    public void generateRawRecommendations(String project, List<QueryHistory> queryHistories, boolean isManual) {
        if (queryHistories == null || queryHistories.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Semi-Auto-Mode project:{} generate suggestions by sqlList size: {}", project, queryHistories.size());
        List<String> sqlList = Lists.newArrayList();
        ListMultimap<String, QueryHistory> queryHistoryMap = ArrayListMultimap.create();
        queryHistories.forEach(queryHistory -> {
            sqlList.add(queryHistory.getSql());
            queryHistoryMap.put(queryHistory.getSql(), queryHistory);
        });

        AbstractContext semiContextV2 = ProposerJob.genOptRec(KylinConfig.getInstanceFromEnv(), project,
                sqlList.toArray(new String[0]));

        Map<String, RawRecItem> nonLayoutRecItemMap = semiContextV2.getExistingNonLayoutRecItemMap();
        transferAndSaveModelRelatedRecItems(semiContextV2, nonLayoutRecItemMap);

        ArrayListMultimap<String, QueryHistory> layoutToQHMap = ArrayListMultimap.create();
        for (AccelerateInfo accelerateInfo : semiContextV2.getAccelerateInfoMap().values()) {
            for (AccelerateInfo.QueryLayoutRelation layout : accelerateInfo.getRelatedLayouts()) {
                List<QueryHistory> queryHistoryList = queryHistoryMap.get(layout.getSql());
                layoutToQHMap.putAll(layout.getModelId() + "_" + layout.getLayoutId(), queryHistoryList);
            }
        }

        List<RawRecItem> layoutRecItems = transferToLayoutRecItems(semiContextV2, layoutToQHMap, nonLayoutRecItemMap);

        if (!isManual && QueryHistoryTaskScheduler.getInstance(project).isInterruptByUser()) {
            throw new IllegalStateException(RawRecService.ACCELERATION_INTERRUPT_BY_USER);
        }
        saveLayoutRawRecItems(layoutRecItems, project);

        markFailAccelerateMessageToQueryHistory(queryHistoryMap, semiContextV2);

        log.info("Semi-Auto-Mode project:{} generate suggestions cost {}ms", project,
                System.currentTimeMillis() - startTime);
    }

    private void transferAndSaveModelRelatedRecItems(AbstractContext semiContext,
            Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        List<RawRecItem> ccRawRecItems = transferToCCRawRecItem(semiContext, nonLayoutUniqueFlagRecMap);
        saveCCRawRecItems(ccRawRecItems, semiContext.getProject());
        ccRawRecItems.forEach(recItem -> nonLayoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItems(semiContext, nonLayoutUniqueFlagRecMap);
        List<RawRecItem> measureRecItems = transferToMeasureRecItems(semiContext, nonLayoutUniqueFlagRecMap);
        saveDimensionAndMeasure(dimensionRecItems, measureRecItems, semiContext.getProject());
        dimensionRecItems.forEach(recItem -> nonLayoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));
        measureRecItems.forEach(recItem -> nonLayoutUniqueFlagRecMap.put(recItem.getUniqueFlag(), recItem));
    }

    public void markFailAccelerateMessageToQueryHistory(ListMultimap<String, QueryHistory> queryHistoryMap,
            AbstractContext semiContextV2) {
        List<Pair<Long, QueryHistoryInfo>> idToQHInfoList = Lists.newArrayList();
        semiContextV2.getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            if (!accelerateInfo.isNotSucceed()) {
                return;
            }
            queryHistoryMap.get(sql).forEach(qh -> {
                QueryHistoryInfo queryHistoryInfo = qh.getQueryHistoryInfo();
                if (queryHistoryInfo == null) {
                    queryHistoryInfo = new QueryHistoryInfo();
                }
                if (accelerateInfo.isFailed()) {
                    String failMessage = accelerateInfo.getFailedCause().getMessage();
                    if (failMessage.length() > 256) {
                        failMessage = failMessage.substring(0, 256);
                    }
                    queryHistoryInfo.setErrorMsg(failMessage);
                } else if (accelerateInfo.isPending()) {
                    queryHistoryInfo.setErrorMsg(accelerateInfo.getPendingMsg());
                }
                idToQHInfoList.add(new Pair<>(qh.getId(), queryHistoryInfo));
            });
        });
        RDBMSQueryHistoryDAO.getInstance().batchUpdateQueryHistoriesInfo(idToQHInfoList);
    }

    public void updateCostsAndTopNCandidates(String projectName) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        EpochManager epochMgr = EpochManager.getInstance(kylinConfig);

        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        List<ProjectInstance> projectList = Lists.newArrayList();
        if (StringUtils.isEmpty(projectName)) {
            List<ProjectInstance> instances = projectManager.listAllProjects().stream() //
                    .filter(projectInstance -> !projectInstance.isExpertMode()) //
                    .collect(Collectors.toList());
            projectList.addAll(instances);
        } else {
            ProjectInstance instance = projectManager.getProject(projectName);
            projectList.add(instance);
        }

        for (ProjectInstance projectInstance : projectList) {
            String project = projectInstance.getName();
            if (!kylinConfig.isUTEnv() && !epochMgr.checkEpochOwner(project)) {
                continue;
            }
            try {
                log.info("Running update cost for project<{}>", project);
                RawRecManager rawRecManager = RawRecManager.getInstance(project);
                NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
                rawRecManager.updateAllCost(project);
                int topN = recommendationSize(project);
                for (String model : projectInstance.getModels()) {
                    long current = System.currentTimeMillis();
                    NDataModel dataModel = modelManager.getDataModelDesc(model);
                    if (dataModel.isBroken()) {
                        log.warn("Broken model({}/{}) cannot update recommendations.", project, model);
                        continue;
                    }

                    log.info("Running update topN raw recommendation for model({}/{}).", project, model);
                    rawRecManager.updateRecommendedTopN(project, model, topN);
                    updateRecommendationCount(project, model);
                    log.info("Update topN raw recommendations for model({}/{}) takes {} ms", //
                            project, model, System.currentTimeMillis() - current);
                }
            } catch (Exception e) {
                log.error("Update cost and update topN failed for project({})", project, e);
            }
        }
    }

    private void updateRecommendationCount(String project, String model) {
        String recActionType = OptRecService.RecActionType.ALL.name();
        int recCount = optRecService.getOptRecLayoutsResponse(project, model, recActionType).getSize();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.updateDataModel(model, copyForWrite -> copyForWrite.setRecommendationsCount(recCount));
            return null;
        }, project);
    }

    public static int recommendationSize(String project) {
        FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        FavoriteRule favoriteRule = FavoriteRule.getDefaultRule(
                ruleManager.getByName(FavoriteRule.REC_SELECT_RULE_NAME), FavoriteRule.REC_SELECT_RULE_NAME);
        FavoriteRule.Condition condition = (FavoriteRule.Condition) favoriteRule.getConds().get(0);
        return Integer.parseInt(condition.getRightThreshold());
    }

    List<RawRecItem> transferToLayoutRecItems(AbstractContext semiContextV2,
            ArrayListMultimap<String, QueryHistory> layoutToQHMap, Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        RawRecManager recManager = RawRecManager.getInstance(semiContextV2.getProject());
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        String recSource = layoutToQHMap.isEmpty() ? RawRecItem.IMPORTED : RawRecItem.QUERY_HISTORY;
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            NDataModel targetModel = modelContext.getTargetModel();
            if (targetModel == null) {
                continue;
            }

            /* For unique string of layout may too long, so it is designed by a uuid,
             * therefore, we need a HashMap to avoid one LayoutRecItemV2 maps to different RawRecItems.
             */
            Map<String, RawRecItem> layoutUniqueFlagRecMap = recManager
                    .queryNonAppliedLayoutRawRecItems(targetModel.getUuid(), true);
            Map<String, String> uniqueContentToUniqueFlagMap = Maps.newHashMap();
            layoutUniqueFlagRecMap.forEach((uniqueFlag, layoutRecItem) -> {
                LayoutRecItemV2 recEntity = (LayoutRecItemV2) layoutRecItem.getRecEntity();
                uniqueContentToUniqueFlagMap.put(recEntity.getLayout().genUniqueContent(), uniqueFlag);
            });

            modelContext.getIndexRexItemMap().forEach((itemUUID, layoutItem) -> {
                // update layout content first
                layoutItem.updateLayoutContent(targetModel, nonLayoutUniqueFlagRecMap);
                RawRecItem recItem;
                String uniqueContent = layoutItem.getLayout().genUniqueContent();
                String uniqueFlag = uniqueContentToUniqueFlagMap.get(uniqueContent);
                if (uniqueContentToUniqueFlagMap.containsKey(uniqueContent)) {
                    recItem = layoutUniqueFlagRecMap.get(uniqueFlag);
                    recItem.setUpdateTime(System.currentTimeMillis());
                    recItem.restoreIfNeed();
                } else {
                    recItem = new RawRecItem(semiContextV2.getProject(), //
                            targetModel.getUuid(), //
                            targetModel.getSemanticVersion(), //
                            RawRecItem.RawRecType.ADDITIONAL_LAYOUT);
                    recItem.setRecEntity(layoutItem);
                    recItem.setCreateTime(layoutItem.getCreateTime());
                    recItem.setUpdateTime(layoutItem.getCreateTime());
                    recItem.setState(RawRecItem.RawRecState.INITIAL);
                    recItem.setUniqueFlag(layoutItem.getUuid());
                }
                recItem.setDependIDs(layoutItem.genDependIds());
                recItem.setRecSource(recSource);
                if (recSource.equalsIgnoreCase(RawRecItem.IMPORTED)) {
                    recItem.cleanLayoutStatistics();
                    recItem.setState(RawRecItem.RawRecState.RECOMMENDED);
                }
                updateLayoutStatistic(recItem, layoutToQHMap, layoutItem.getLayout(), targetModel);
                if (recItem.isAdditionalRecItemSavable()) {
                    rawRecItems.add(recItem);
                }
            });
        }
        return rawRecItems;
    }

    private void updateLayoutStatistic(RawRecItem recItem, ListMultimap<String, QueryHistory> layoutToQHMap,
            LayoutEntity layout, NDataModel targetModel) {
        if (layoutToQHMap.isEmpty()) {
            return;
        }
        List<QueryHistory> queryHistories = layoutToQHMap.get(targetModel.getId() + "_" + layout.getId());
        if (CollectionUtils.isEmpty(queryHistories)) {
            return;
        }
        LayoutMetric layoutMetric = recItem.getLayoutMetric();
        if (layoutMetric == null) {
            layoutMetric = new LayoutMetric(new FrequencyMap(), new LayoutMetric.LatencyMap());
        }

        LayoutMetric.LatencyMap latencyMap = layoutMetric.getLatencyMap();
        FrequencyMap frequencyMap = layoutMetric.getFrequencyMap();
        double totalTime = recItem.getTotalTime();
        double maxTime = recItem.getMaxTime();
        long minTime = Long.MAX_VALUE;
        int hitCount = recItem.getHitCount();
        for (QueryHistory qh : queryHistories) {
            hitCount++;
            long duration = qh.getDuration();
            totalTime = totalTime + duration;
            if (duration > maxTime) {
                maxTime = duration;
            }
            if (duration < minTime) {
                minTime = duration;
            }

            latencyMap.incLatency(qh.getQueryTime(), duration);
            frequencyMap.incFrequency(qh.getQueryTime());
        }
        recItem.setTotalTime(totalTime);
        recItem.setMaxTime(maxTime);
        recItem.setMinTime(minTime);

        recItem.setLayoutMetric(layoutMetric);
        recItem.setHitCount(hitCount);
    }

    private List<RawRecItem> transferToMeasureRecItems(AbstractContext semiContextV2,
            Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }
            modelContext.getMeasureRecItemMap().forEach((uniqueFlag, measureItem) -> {
                RawRecItem item;
                if (nonLayoutUniqueFlagRecMap.containsKey(uniqueFlag)) {
                    item = nonLayoutUniqueFlagRecMap.get(uniqueFlag);
                    item.setUpdateTime(System.currentTimeMillis());
                } else {
                    item = new RawRecItem(semiContextV2.getProject(), //
                            modelContext.getTargetModel().getUuid(), //
                            modelContext.getTargetModel().getSemanticVersion(), //
                            RawRecItem.RawRecType.MEASURE);
                    item.setUniqueFlag(uniqueFlag);
                    item.setState(RawRecItem.RawRecState.INITIAL);
                    item.setCreateTime(measureItem.getCreateTime());
                    item.setUpdateTime(measureItem.getCreateTime());
                    item.setRecEntity(measureItem);
                }
                item.setDependIDs(measureItem.genDependIds(nonLayoutUniqueFlagRecMap, measureItem.getUniqueContent(),
                        modelContext.getOriginModel()));
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToDimensionRecItems(AbstractContext semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }
            modelContext.getDimensionRecItemMap().forEach((uniqueFlag, dimItem) -> {
                RawRecItem item;
                if (uniqueRecItemMap.containsKey(uniqueFlag)) {
                    item = uniqueRecItemMap.get(uniqueFlag);
                    item.setUpdateTime(System.currentTimeMillis());
                } else {
                    item = new RawRecItem(semiContextV2.getProject(), //
                            modelContext.getTargetModel().getUuid(), //
                            modelContext.getTargetModel().getSemanticVersion(), //
                            RawRecItem.RawRecType.DIMENSION);
                    item.setUniqueFlag(uniqueFlag);
                    item.setCreateTime(dimItem.getCreateTime());
                    item.setUpdateTime(dimItem.getCreateTime());
                    item.setState(RawRecItem.RawRecState.INITIAL);
                    item.setRecEntity(dimItem);
                }
                item.setDependIDs(dimItem.genDependIds(uniqueRecItemMap, dimItem.getUniqueContent(),
                        modelContext.getOriginModel()));
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToCCRawRecItem(AbstractContext semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.ModelContext modelContext : semiContextV2.getModelContexts()) {
            if (modelContext.getTargetModel() == null) {
                continue;
            }
            modelContext.getCcRecItemMap().forEach((uniqueFlag, ccItem) -> {
                RawRecItem item;
                if (uniqueRecItemMap.containsKey(uniqueFlag)) {
                    item = uniqueRecItemMap.get(uniqueFlag);
                    item.setUpdateTime(System.currentTimeMillis());
                } else {
                    item = new RawRecItem(semiContextV2.getProject(), //
                            modelContext.getTargetModel().getUuid(), //
                            modelContext.getTargetModel().getSemanticVersion(), //
                            RawRecItem.RawRecType.COMPUTED_COLUMN);
                    item.setCreateTime(ccItem.getCreateTime());
                    item.setUpdateTime(ccItem.getCreateTime());
                    item.setUniqueFlag(uniqueFlag);
                    item.setRecEntity(ccItem);
                    item.setState(RawRecItem.RawRecState.INITIAL);
                }
                item.setDependIDs(ccItem.genDependIds(modelContext.getOriginModel()));
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private void saveCCRawRecItems(List<RawRecItem> ccRawRecItems, String project) {
        RawRecManager.getInstance(project).saveOrUpdate(ccRawRecItems);
    }

    private void saveDimensionAndMeasure(List<RawRecItem> dimensionRecItems, List<RawRecItem> measureRecItems,
            String project) {
        List<RawRecItem> recItems = Lists.newArrayList();
        recItems.addAll(dimensionRecItems);
        recItems.addAll(measureRecItems);
        RawRecManager.getInstance(project).saveOrUpdate(recItems);
    }

    private void saveLayoutRawRecItems(List<RawRecItem> layoutRecItems, String project) {
        RawRecManager.getInstance(project).saveOrUpdate(layoutRecItems);
    }

}
