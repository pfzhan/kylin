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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecSelection;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecommendationManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiContextV2;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("rawRecommendationService")
public class RawRecommendationService {

    public void generateRawRecommendations(String project, List<QueryHistory> queryHistories) {
        if (queryHistories == null || queryHistories.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Semi-Auto-Mode project:{} generate suggestions by sqlList size: {}", project, queryHistories.size());
        List<String> sqlList = Lists.newArrayList();
        List<Long> queryIDList = Lists.newArrayList();
        queryHistories.forEach(queryHistory -> {
            sqlList.add(queryHistory.getSql());
            queryIDList.add(queryHistory.getId());
        });

        AbstractSemiContextV2 semiContextV2 = NSmartMaster.genOptRecommendationSemiV2(KylinConfig.getInstanceFromEnv(),
                project, sqlList.toArray(new String[0]), null);

        List<RawRecItem> ccRawRecItems = transferToCCRawRecItem(semiContextV2);
        Map<String, RawRecItem> savedCCRawRecItems = mockSaveCCRawRecItems(ccRawRecItems);

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItems(semiContextV2, savedCCRawRecItems);
        List<RawRecItem> measureRecItems = transferToMeasureRecItems(semiContextV2, savedCCRawRecItems);
        Map<Integer, Integer> savedDimensionAndMeasure = mockSaveDimensionAndMeasure(dimensionRecItems,
                measureRecItems);

        List<RawRecItem> layoutRecItems = transferToLayoutRecItems(semiContextV2, savedDimensionAndMeasure,
                queryIDList);
        mockSaveLayoutRawRecItems(layoutRecItems);

        log.info("Semi-Auto-Mode project:{} generate suggestions cost {}ms", project,
                System.currentTimeMillis() - startTime);
    }

    public void updateCostAndSelectTopRec() {
        updateCost();
        selectTopRec();
    }

    private void updateCost() {

        for (ProjectInstance projectInstance : NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects()) {
            RawRecommendationManager.getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName())
                    .updateAllCost(projectInstance.getName());
        }
    }

    private void selectTopRec() {
        for (ProjectInstance projectInstance : NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects()) {
            for (String model : projectInstance.getModels()) {
                var recLimitRule = FavoriteRule.getDefaultRule(
                        FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName())
                                .getByName(FavoriteRule.RECOMMENDATION_RULE_NAME),
                        FavoriteRule.RECOMMENDATION_RULE_NAME);
                int semanticVersion = NDataModelManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName())
                        .getDataModelDesc(model).getSemanticVersion();
                int limit = Integer
                        .parseInt(((FavoriteRule.Condition) recLimitRule.getConds().get(0)).getRightThreshold());
                List<RawRecItem> bestItem = RawRecSelection.getInstance().selectBestLayout(limit,
                        projectInstance.getName(), model, semanticVersion);
                // TODO saveToMetadata
            }
        }
    }

    public void updateCost(String project) {

    }

    private List<RawRecItem> transferToLayoutRecItems(AbstractSemiContextV2 semiContextV2,
            Map<Integer, Integer> savedDimensionAndMeasure, List<Long> queryIDList) {
        //todo manager update already existing items
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getIndexRexItemMap().forEach((colOrder, layoutItem) -> {
                RawRecItem recItem = new RawRecItem(semiContextV2.getProject(), //
                        modelContext.getTargetModel().getUuid(), //
                        modelContext.getTargetModel().getSemanticVersion(), //
                        RawRecItem.RawRecType.LAYOUT);
                recItem.setRecEntity(layoutItem);
                recItem.setCreateTime(layoutItem.getCreateTime());
                // use dbColOrder instead
                int[] colOrderInDB = new int[colOrder.size()];
                for (int i = 0; i < colOrder.size(); i++) {
                    final Integer id = colOrder.get(i);
                    if (savedDimensionAndMeasure.containsKey(id)) {
                        colOrderInDB[i] = -1 * savedDimensionAndMeasure.get(id);
                    } else {
                        colOrderInDB[i] = id;
                    }
                }
                recItem.setUniqueFlag(Arrays.toString(colOrderInDB));
                recItem.setDependIDs(colOrderInDB);
                //todo: add other statistics

                rawRecItems.add(recItem);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToMeasureRecItems(AbstractSemiContextV2 semiContextV2,
            Map<String, RawRecItem> ccRecItemMap) {
        //todo manager filter out already existing items
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getMeasureRecItemMap().forEach((name, measureItem) -> {
                RawRecItem recItem = new RawRecItem(semiContextV2.getProject(), //
                        modelContext.getTargetModel().getUuid(), //
                        modelContext.getTargetModel().getSemanticVersion(), //
                        RawRecItem.RawRecType.MEASURE);
                recItem.setUniqueFlag(name);
                recItem.setRecEntity(measureItem);
                recItem.setCreateTime(measureItem.getCreateTime());
                String[] params = name.split("__");
                int[] dependID = new int[params.length - 1];
                for (String param : params) {
                    if (ccRecItemMap.containsKey(param)) {
                        recItem.setDependIDs(dependID);
                    }

                }
                rawRecItems.add(recItem);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToDimensionRecItems(AbstractSemiContextV2 semiContextV2,
            Map<String, RawRecItem> ccRecItemMap) {
        //todo manager filter out already existing items
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getDimensionRecItemMap().forEach((name, dimItem) -> {
                RawRecItem recItem = new RawRecItem(semiContextV2.getProject(), //
                        modelContext.getTargetModel().getUuid(), //
                        modelContext.getTargetModel().getSemanticVersion(), //
                        RawRecItem.RawRecType.DIMENSION);
                recItem.setUniqueFlag(name);
                recItem.setRecEntity(dimItem);
                recItem.setCreateTime(dimItem.getCreateTime());
                if (ccRecItemMap.containsKey(name)) {
                    recItem.setDependIDs(new int[] { ccRecItemMap.get(name).getId() });
                }

                rawRecItems.add(recItem);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToCCRawRecItem(AbstractSemiContextV2 semiContextV2) {
        //todo manager filter out already existing items
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getCcRecItemMap().forEach((innerExp, ccItem) -> {
                RawRecItem recItem = new RawRecItem(semiContextV2.getProject(), //
                        modelContext.getTargetModel().getUuid(), //
                        modelContext.getTargetModel().getSemanticVersion(), //
                        RawRecItem.RawRecType.COMPUTED_COLUMN);
                recItem.setUniqueFlag(innerExp);
                recItem.setRecEntity(ccItem);
                recItem.setCreateTime(ccItem.getCreateTime());
                rawRecItems.add(recItem);
            });
        }
        return rawRecItems;
    }

    private Map<String, RawRecItem> mockSaveCCRawRecItems(List<RawRecItem> ccRawRecItems) {
        //todo
        return Maps.newHashMap();
    }

    private Map<Integer, Integer> mockSaveDimensionAndMeasure(List<RawRecItem> dimensionRecItems,
            List<RawRecItem> measureRecItems) {
        //todo
        return Maps.newHashMap();
    }

    private void mockSaveLayoutRawRecItems(List<RawRecItem> layoutRecItems) {
        //todo 
    }

}
