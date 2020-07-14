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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.stereotype.Component;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.recommendation.candidate.LayoutMetric;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecSelection;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.v2.OptimizeRecommendationManagerV2;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiContextV2;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("rawRecService")
public class RawRecService {
    private static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24L;

    public void generateRawRecommendations(String project, List<QueryHistory> queryHistories) {
        if (queryHistories == null || queryHistories.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Semi-Auto-Mode project:{} generate suggestions by sqlList size: {}", project, queryHistories.size());
        List<String> sqlList = Lists.newArrayList();
        ArrayListMultimap<String, QueryHistory> queryHistoryMap = ArrayListMultimap.create();
        queryHistories.forEach(queryHistory -> {
            sqlList.add(queryHistory.getSql());
            queryHistoryMap.put(queryHistory.getSql(), queryHistory);
        });

        AbstractSemiContextV2 semiContextV2 = NSmartMaster.genOptRecommendationSemiV2(KylinConfig.getInstanceFromEnv(),
                project, sqlList.toArray(new String[0]), null);

        List<RawRecItem> ccRawRecItems = transferToCCRawRecItem(semiContextV2);
        saveCCRawRecItems(ccRawRecItems, project);

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItems(semiContextV2);
        List<RawRecItem> measureRecItems = transferToMeasureRecItems(semiContextV2);
        saveDimensionAndMeasure(dimensionRecItems, measureRecItems, project);

        ArrayListMultimap<Long, QueryHistory> layoutToQHMap = ArrayListMultimap.create();
        for (AccelerateInfo accelerateInfo : semiContextV2.getAccelerateInfoMap().values()) {
            for (AccelerateInfo.QueryLayoutRelation layout : accelerateInfo.getRelatedLayouts()) {
                List<QueryHistory> queryHistoryList = queryHistoryMap.get(layout.getSql());
                layoutToQHMap.putAll(layout.getLayoutId(), queryHistoryList);
            }
        }

        List<RawRecItem> layoutRecItems = transferToLayoutRecItems(semiContextV2, layoutToQHMap);
        saveLayoutRawRecItems(layoutRecItems, project);

        log.info("Semi-Auto-Mode project:{} generate suggestions cost {}ms", project,
                System.currentTimeMillis() - startTime);
    }

    public void updateCostAndSelectTopRec() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        EpochManager epochMgr = EpochManager.getInstance(kylinConfig);
        RawRecSelection rawRecSelection = RawRecSelection.getInstance();
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(kylinConfig) //
                .listAllProjects().stream() //
                .filter(projectInstance -> !projectInstance.isExpertMode()) //
                .collect(Collectors.toList());
        for (ProjectInstance projectInstance : projectInstances) {
            String project = projectInstance.getName();
//            if (!kylinConfig.isUTEnv() && !epochMgr.checkEpochOwner(project)) {
//                continue;
//            }
            try {
                log.info("Running update cost for project<{}>", project);
                RawRecManager.getInstance(project).updateAllCost(project);
                FavoriteRule favoriteRule = FavoriteRuleManager.getInstance(kylinConfig, project)
                        .getByName(FavoriteRule.REC_SELECT_RULE_NAME);
                for (String model : projectInstance.getModels()) {
                    log.info("Running select topN recommendation for {}/({}).", project, model);
                    int topN = Integer.parseInt(((FavoriteRule.Condition) FavoriteRule
                            .getDefaultRule(favoriteRule, FavoriteRule.REC_SELECT_RULE_NAME).getConds().get(0))
                                    .getRightThreshold());
                    List<Integer> bestItemsIds = rawRecSelection.selectBestLayout(topN, model, project).stream()
                            .map(RawRecItem::getId).collect(Collectors.toList());
                    updateRecommendationV2(project, model, bestItemsIds);
                }
            } catch (Exception e) {
                log.error("Update cost and select topN failed for project<{}>", project, e);
            }
        }
    }

    List<RawRecItem> transferToLayoutRecItems(AbstractSemiContextV2 semiContextV2,
            ArrayListMultimap<Long, QueryHistory> layoutToQHMap) {
        val mgr = RawRecManager.getInstance(semiContextV2.getProject());
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            NDataModel targetModel = modelContext.getTargetModel();
            if (targetModel == null) {
                continue;
            }
            Map<String, RawRecItem> layoutRecommendations = mgr.queryLayoutRawRecItems(targetModel.getUuid());
            Map<String, String> uniqueFlagToUuid = Maps.newHashMap();
            layoutRecommendations.forEach((k, v) -> {
                LayoutRecItemV2 recEntity = (LayoutRecItemV2) v.getRecEntity();
                uniqueFlagToUuid.put(recEntity.getLayout().genUniqueFlag(), k);
            });

            modelContext.getIndexRexItemMap().forEach((colOrder, layoutItem) -> {
                layoutItem.updateLayoutInfo(targetModel);
                String uniqueString = layoutItem.getLayout().genUniqueFlag();
                String uuid = uniqueFlagToUuid.get(uniqueString);
                RawRecItem recItem;
                if (uniqueFlagToUuid.containsKey(uniqueString)) {
                    recItem = layoutRecommendations.get(uuid);
                    recItem.setUpdateTime(System.currentTimeMillis());
                    if (recItem.getState() == RawRecItem.RawRecState.DISCARD) {
                        recItem.setState(RawRecItem.RawRecState.INITIAL);
                    }
                } else {
                    recItem = new RawRecItem(semiContextV2.getProject(), //
                            targetModel.getUuid(), //
                            targetModel.getSemanticVersion(), //
                            RawRecItem.RawRecType.LAYOUT);
                    recItem.setRecEntity(layoutItem);
                    recItem.setCreateTime(layoutItem.getCreateTime());
                    recItem.setUpdateTime(layoutItem.getCreateTime());
                    recItem.setState(RawRecItem.RawRecState.INITIAL);
                    recItem.setUniqueFlag(layoutItem.getUuid());
                    recItem.setDependIDs(layoutItem.genDependIds());
                }
                updateLayoutStatistic(recItem, layoutToQHMap, layoutItem.getLayout().getId());
                if (recItem.getLayoutMetric() != null) {
                    rawRecItems.add(recItem);
                }
            });
        }
        return rawRecItems;
    }

    private void updateLayoutStatistic(RawRecItem recItem, ArrayListMultimap<Long, QueryHistory> layoutToQHMap,
            long layoutId) {
        List<QueryHistory> queryHistories = layoutToQHMap.get(layoutId);
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

        recItem.setTotalLatencyOfLastDay(latencyMap.getLatencyByDate(System.currentTimeMillis() - MILLIS_PER_DAY));
        recItem.setLayoutMetric(layoutMetric);
        recItem.setHitCount(hitCount);
    }

    private List<RawRecItem> transferToMeasureRecItems(AbstractSemiContextV2 semiContextV2) {
        val mgr = RawRecManager.getInstance(semiContextV2.getProject());
        Map<String, RawRecItem> uniqueRecItemMap = mgr.listAll();
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getMeasureRecItemMap().forEach((uniqueFlag, measureItem) -> {
                RawRecItem item;
                if (uniqueRecItemMap.containsKey(uniqueFlag)) {
                    item = uniqueRecItemMap.get(uniqueFlag);
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
                    item.setDependIDs(measureItem.genDependIds(uniqueRecItemMap, uniqueFlag));
                }
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToDimensionRecItems(AbstractSemiContextV2 semiContextV2) {
        val rcMgr = RawRecManager.getInstance(semiContextV2.getProject());
        Map<String, RawRecItem> uniqueRecItemMap = rcMgr.listAll();
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
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
                    item.setDependIDs(dimItem.genDependIds(uniqueRecItemMap, uniqueFlag.split("__")[1]));
                }
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToCCRawRecItem(AbstractSemiContextV2 semiContextV2) {
        val rcMgr = RawRecManager.getInstance(semiContextV2.getProject());
        Map<String, RawRecItem> uniqueRecItemMap = rcMgr.listAll();
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
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
                    item.setDependIDs(ccItem.genDependIds(modelContext.getTargetModel()));
                }

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

    private void updateRecommendationV2(String project, String modelId, List<Integer> rawIds) {
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                OptimizeRecommendationManagerV2 managerV2 = OptimizeRecommendationManagerV2
                        .getInstance(KylinConfig.getInstanceFromEnv(), project);
                managerV2.createOrUpdate(modelId, rawIds);
                return null;
            }, project);
        } catch (Exception e) {
            log.error("project<" + project + "> model<" + modelId + ">failed to update RecommendationV2 file.", e);
        }
    }

    public void deleteRawRecItems() {
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects().stream().filter(projectInstance -> !projectInstance.isExpertMode())
                .collect(Collectors.toList());
        Thread.currentThread().setName("DeleteRawRecItemsInDB");
        for (ProjectInstance instance : projectInstances) {
            try {
                RawRecManager.getInstance(instance.getName())
                        .deleteAllOutDated(instance.getName());
            } catch (Exception e) {
                log.error("project<" + instance.getName() + "> delete raw recommendations in DB failed", e);
            }
        }
    }
}
