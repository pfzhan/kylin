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
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
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
public class RawRecommendationService extends BasicService {

    public void generateRawRecommendations(String project, Map<Long, String> queries) {
        if (MapUtils.isEmpty(queries)) {
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Semi-Auto-Mode project:{} generate suggestions by sqlList size: {}", project, queries.size());
        Map<String, Set<Long>> contentToIDMap = Maps.newHashMap();
        queries.forEach((queryID, queryContent) -> {
            contentToIDMap.putIfAbsent(queryContent, Sets.newHashSet());
            contentToIDMap.get(queryContent).add(queryID);
        });

        List<String> sqlList = Lists.newArrayList();
        List<Set<Long>> queryIDList = Lists.newArrayList();
        contentToIDMap.forEach((queryContent, idSet) -> {
            sqlList.add(queryContent);
            queryIDList.add(idSet);
        });

        AbstractSemiContextV2 semiContextV2 = NSmartMaster.genOptRecommendationSemiV2(KylinConfig.getInstanceFromEnv(),
                project, sqlList.toArray(new String[0]), null);

        List<RawRecItem> ccRawRecItems = transferToCCRawRecItem(semiContextV2);
        Map<String, RawRecItem> savedCCRawRecItems = mockSaveCCRawRecItems(ccRawRecItems);

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItems(semiContextV2, savedCCRawRecItems);
        List<RawRecItem> measureRecItems = transferToMeasureRecItems(semiContextV2, savedCCRawRecItems);
        Map<Integer, Long> savedDimensionAndMeasure = mockSaveDimensionAndMeasure(dimensionRecItems, measureRecItems);

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
                int limit = Integer
                        .valueOf(((FavoriteRule.Condition) recLimitRule.getConds().get(0)).getRightThreshold());
                List<RawRecItem> bestItem = RawRecSelection.getInstance().selectBestLayout(limit,
                        projectInstance.getName(), model);
                // TODO saveToMetadata
            }
        }
    }

    public void updateCost(String project) {

    }

    private List<RawRecItem> transferToLayoutRecItems(AbstractSemiContextV2 semiContextV2,
            Map<Integer, Long> savedDimensionAndMeasure, List<Set<Long>> queryIDList) {
        //todo manager update already existing items
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getIndexRexItemMap().forEach((colOrder, layoutItem) -> {
                RawRecItem recItem = new RawRecItem(semiContextV2.getProject(), //
                        modelContext.getTargetModel().getUuid(), //
                        modelContext.getTargetModel().getSemanticVersion(), //
                        RawRecItem.RawRecType.LAYOUT);
                recItem.setEntity(layoutItem);
                recItem.setCreateTime(layoutItem.getCreateTime());
                // use dbColOrder instead
                long[] colOrderInDB = new long[colOrder.size()];
                for (int i = 0; i < colOrder.size(); i++) {
                    final Integer id = colOrder.get(i);
                    if (savedDimensionAndMeasure.containsKey(id)) {
                        colOrderInDB[i] = -1 * savedDimensionAndMeasure.get(id);
                    } else {
                        colOrderInDB[i] = id;
                    }
                }
                recItem.setUniqueFlag(Arrays.toString(colOrderInDB));
                recItem.setDependID(colOrderInDB);
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
                recItem.setEntity(measureItem);
                recItem.setCreateTime(measureItem.getCreateTime());
                String[] params = name.split("__");
                long[] dependID = new long[params.length - 1];
                for (String param : params) {
                    if (ccRecItemMap.containsKey(param)) {
                        recItem.setDependID(dependID);
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
                recItem.setEntity(dimItem);
                recItem.setCreateTime(dimItem.getCreateTime());
                if (ccRecItemMap.containsKey(name)) {
                    recItem.setDependID(new long[] { ccRecItemMap.get(name).getId() });
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
                recItem.setEntity(ccItem);
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

    private Map<Integer, Long> mockSaveDimensionAndMeasure(List<RawRecItem> dimensionRecItems,
            List<RawRecItem> measureRecItems) {
        //todo
        return Maps.newHashMap();
    }

    private void mockSaveLayoutRawRecItems(List<RawRecItem> layoutRecItems) {
        //todo 
    }

}
