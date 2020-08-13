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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.LayoutMetric;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiContextV2;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("rawRecService")
public class RawRecService {

    @Autowired
    ProjectService projectService;

    public void accelerate(String project) {
        projectService.accelerateImmediately(project);
        updateCostsAndTopNCandidates();
    }

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

        RawRecManager rawRecManager = RawRecManager.getInstance(semiContextV2.getProject());
        Set<String> relatedModels = Sets.newHashSet();
        semiContextV2.getModelContexts().forEach(context -> {
            if (context.getTargetModel() == null) {
                return;
            }
            relatedModels.add(context.getTargetModel().getUuid());
        });
        Map<String, RawRecItem> nonLayoutRecItemMap = rawRecManager.queryNonLayoutRecItems(relatedModels);

        List<RawRecItem> ccRawRecItems = transferToCCRawRecItem(semiContextV2, nonLayoutRecItemMap);
        saveCCRawRecItems(ccRawRecItems, project);
        ccRawRecItems.forEach(recItem -> nonLayoutRecItemMap.put(recItem.getUniqueFlag(), recItem));

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItems(semiContextV2, nonLayoutRecItemMap);
        List<RawRecItem> measureRecItems = transferToMeasureRecItems(semiContextV2, nonLayoutRecItemMap);
        saveDimensionAndMeasure(dimensionRecItems, measureRecItems, project);
        dimensionRecItems.forEach(recItem -> nonLayoutRecItemMap.put(recItem.getUniqueFlag(), recItem));
        measureRecItems.forEach(recItem -> nonLayoutRecItemMap.put(recItem.getUniqueFlag(), recItem));

        ArrayListMultimap<String, QueryHistory> layoutToQHMap = ArrayListMultimap.create();
        for (AccelerateInfo accelerateInfo : semiContextV2.getAccelerateInfoMap().values()) {
            for (AccelerateInfo.QueryLayoutRelation layout : accelerateInfo.getRelatedLayouts()) {
                List<QueryHistory> queryHistoryList = queryHistoryMap.get(layout.getSql());
                layoutToQHMap.putAll(layout.getModelId() + "_" + layout.getLayoutId(), queryHistoryList);
            }
        }

        List<RawRecItem> layoutRecItems = transferToLayoutRecItems(semiContextV2, layoutToQHMap, nonLayoutRecItemMap);
        saveLayoutRawRecItems(layoutRecItems, project);

        markFailAccelerateMessageToQueryHistory(queryHistoryMap, semiContextV2);

        log.info("Semi-Auto-Mode project:{} generate suggestions cost {}ms", project,
                System.currentTimeMillis() - startTime);
    }

    public void markFailAccelerateMessageToQueryHistory(ArrayListMultimap<String, QueryHistory> queryHistoryMap,
            AbstractSemiContextV2 semiContextV2) {
        List<Pair<Long, QueryHistoryInfo>> idToQHInfoList = Lists.newArrayList();
        semiContextV2.getAccelerateInfoMap().entrySet().stream().filter(entry -> entry.getValue().isNotSucceed())
                .forEach(entry -> {
                    queryHistoryMap.get(entry.getKey()).forEach(qh -> {
                        AccelerateInfo accelerateInfo = entry.getValue();
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
        RDBMSQueryHistoryDAO.getInstance(KylinConfig.getInstanceFromEnv()).batchUpdataQueryHistorieInfo(idToQHInfoList);
    }

    public void updateCostsAndTopNCandidates() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        EpochManager epochMgr = EpochManager.getInstance(kylinConfig);
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(kylinConfig) //
                .listAllProjects().stream() //
                .filter(projectInstance -> !projectInstance.isExpertMode()) //
                .collect(Collectors.toList());
        for (ProjectInstance projectInstance : projectInstances) {
            String project = projectInstance.getName();
            if (!kylinConfig.isUTEnv() && !epochMgr.checkEpochOwner(project)) {
                continue;
            }
            try {
                log.info("Running update cost for project<{}>", project);
                val rawRecManager = RawRecManager.getInstance(project);
                rawRecManager.updateAllCost(project);
                FavoriteRule favoriteRule = FavoriteRuleManager.getInstance(kylinConfig, project)
                        .getByName(FavoriteRule.REC_SELECT_RULE_NAME);
                for (String model : projectInstance.getModels()) {
                    long current = System.currentTimeMillis();
                    log.info("Running update topN raw recommendation for {}/({}).", project, model);
                    int topN = Integer.parseInt(((FavoriteRule.Condition) FavoriteRule
                            .getDefaultRule(favoriteRule, FavoriteRule.REC_SELECT_RULE_NAME).getConds().get(0))
                                    .getRightThreshold());
                    rawRecManager.updateRecommendedTopN(project, model, topN);
                    log.info("Update topN raw recommendations for project({})/model({}) takes {} ms", // 
                            project, model, System.currentTimeMillis() - current);
                }
            } catch (Exception e) {
                log.error("Update cost and update topN failed for project<{}>", project, e);
            }
        }
    }

    List<RawRecItem> transferToLayoutRecItems(AbstractSemiContextV2 semiContextV2,
            ArrayListMultimap<String, QueryHistory> layoutToQHMap, Map<String, RawRecItem> recItemMap) {
        RawRecManager recManager = RawRecManager.getInstance(semiContextV2.getProject());
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            NDataModel targetModel = modelContext.getTargetModel();
            if (targetModel == null) {
                continue;
            }

            /* For unique string of layout may too long, so it is designed by a uuid,
             * therefore, we need a HashMap to avoid one LayoutRecItemV2 maps to different RawRecItems.
             */
            Map<String, RawRecItem> layoutRecommendations = recManager.queryLayoutRawRecItems(targetModel.getUuid());
            Map<String, String> uniqueFlagToUuid = Maps.newHashMap();
            layoutRecommendations.forEach((k, v) -> {
                LayoutRecItemV2 recEntity = (LayoutRecItemV2) v.getRecEntity();
                uniqueFlagToUuid.put(recEntity.getLayout().genUniqueFlag(), k);
            });

            modelContext.getIndexRexItemMap().forEach((colOrder, layoutItem) -> {
                layoutItem.updateLayoutInfo(targetModel, recItemMap);
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
                updateLayoutStatistic(recItem, layoutToQHMap, layoutItem.getLayout());
                if (recItem.getLayoutMetric() != null) {
                    rawRecItems.add(recItem);
                }
            });
        }
        return rawRecItems;
    }

    private void updateLayoutStatistic(RawRecItem recItem, ArrayListMultimap<String, QueryHistory> layoutToQHMap,
            LayoutEntity layout) {
        List<QueryHistory> queryHistories = layoutToQHMap.get(layout.getModel().getId() + "_" + layout.getId());
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

    private List<RawRecItem> transferToMeasureRecItems(AbstractSemiContextV2 semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
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
                    item.setDependIDs(
                            measureItem.genDependIds(uniqueRecItemMap, uniqueFlag, modelContext.getOriginModel()));
                }
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToDimensionRecItems(AbstractSemiContextV2 semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
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

    private List<RawRecItem> transferToCCRawRecItem(AbstractSemiContextV2 semiContextV2,
            Map<String, RawRecItem> uniqueRecItemMap) {
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

    public void deleteRawRecItems() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(projectInstance -> !projectInstance.isExpertMode()).collect(Collectors.toList());
        Thread.currentThread().setName("DeleteRawRecItemsInDB");
        for (ProjectInstance instance : projectInstances) {
            try {
                RawRecManager rawRecManager = RawRecManager.getInstance(instance.getName());
                rawRecManager.deleteAllOutDated(instance.getName());
                Set<String> modelIds = NDataModelManager.getInstance(config, instance.getName()).listAllModelIds();
                rawRecManager.deleteRecItemsOfNonExistModels(instance.getName(), modelIds);
            } catch (Exception e) {
                log.error("project<" + instance.getName() + "> delete raw recommendations in DB failed", e);
            }
        }
    }
}
