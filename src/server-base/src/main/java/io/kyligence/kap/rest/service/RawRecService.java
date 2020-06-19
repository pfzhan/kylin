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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecSelection;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.v2.OptimizeRecommendationManagerV2;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiContextV2;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("rawRecService")
public class RawRecService {

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
        saveCCRawRecItems(ccRawRecItems, project);

        List<RawRecItem> dimensionRecItems = transferToDimensionRecItems(semiContextV2);
        List<RawRecItem> measureRecItems = transferToMeasureRecItems(semiContextV2);
        saveDimensionAndMeasure(dimensionRecItems, measureRecItems, project);

        List<RawRecItem> layoutRecItems = transferToLayoutRecItems(semiContextV2, queryIDList);
        saveLayoutRawRecItems(layoutRecItems, project);

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
            RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName())
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
                        .parseInt(((FavoriteRule.Condition) recLimitRule.getConds().get(0)).getRightThreshold());
                List<RawRecItem> bestItem = RawRecSelection.getInstance().selectBestLayout(limit, model,
                        projectInstance.getName());
                updateRecommendationV2(projectInstance.getName(), model,
                        bestItem.stream().map(RawRecItem::getId).collect(Collectors.toList()));
            }
        }
    }

    private List<RawRecItem> transferToLayoutRecItems(AbstractSemiContextV2 semiContextV2, List<Long> queryIDList) {
        val mgr = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), semiContextV2.getProject());
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            NDataModel targetModel = modelContext.getTargetModel();
            Map<String, RawRecItem> layoutRecommendations = mgr.queryLayoutRawRecItems(targetModel.getUuid());
            modelContext.getIndexRexItemMap().forEach((colOrder, layoutItem) -> {
                updateLayoutInfo(layoutItem, targetModel);
                String uniqueFlag = layoutItem.getLayout().genUniqueFlag();
                RawRecItem recItem;
                if (layoutRecommendations.containsKey(uniqueFlag)) {
                    recItem = layoutRecommendations.get(uniqueFlag);
                    recItem.setUpdateTime(System.currentTimeMillis());
                    // update statistics
                } else {
                    recItem = new RawRecItem(semiContextV2.getProject(), //
                            targetModel.getUuid(), //
                            targetModel.getSemanticVersion(), //
                            RawRecItem.RawRecType.LAYOUT);
                    recItem.setRecEntity(layoutItem);
                    recItem.setCreateTime(layoutItem.getCreateTime());
                    recItem.setUpdateTime(layoutItem.getCreateTime());
                    recItem.setState(RawRecItem.RawRecState.INITIAL);
                    recItem.setUniqueFlag(uniqueFlag);
                    recItem.setDependIDs(getColOrderArray(layoutItem.getLayout().getColOrder()));
                    //todo: add other statistics
                }
                rawRecItems.add(recItem);
            });
        }
        return rawRecItems;
    }

    public int[] getColOrderArray(List<Integer> colOrder) {
        int[] arr = new int[colOrder.size()];
        for (int i = 0; i < colOrder.size(); i++) {
            arr[i] = colOrder.get(i);
        }
        return arr;
    }

    private void updateLayoutInfo(LayoutRecItemV2 item, NDataModel dataModel) {
        LayoutEntity layout = item.getLayout();
        Map<String, ComputedColumnDesc> ccMap = getCcOnModels(dataModel);
        List<Integer> colOrderInDB = getColIDInDB(ccMap, dataModel, layout.getColOrder());
        List<Integer> shardColsInDB = getColIDInDB(ccMap, dataModel, layout.getShardByColumns());
        List<Integer> sortColsInDB = getColIDInDB(ccMap, dataModel, layout.getSortByColumns());
        List<Integer> partitionColsInDB = getColIDInDB(ccMap, dataModel, layout.getPartitionByColumns());
        layout.setColOrder(colOrderInDB);
        layout.setShardByColumns(shardColsInDB);
        layout.setSortByColumns(sortColsInDB);
        layout.setPartitionByColumns(partitionColsInDB);
    }

    private List<Integer> getColIDInDB(Map<String, ComputedColumnDesc> ccMap, NDataModel model,
            List<Integer> columnIDs) {
        val uniqueRecItemMap = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .listAll();
        List<Integer> colOrderInDB = Lists.newArrayList(columnIDs.size());
        columnIDs.forEach(colId -> {
            String key;
            if (colId < NDataModel.MEASURE_ID_BASE) {
                final TblColRef tblColRef = model.getEffectiveDimensions().get(colId);
                if (tblColRef.getColumnDesc().isComputedColumn()) {
                    key = tblColRef.getColumnDesc().getComputedColumnExpr();
                } else {
                    key = "d__" + tblColRef.getIdentity();
                }
            } else {
                final NDataModel.Measure measure = model.getEffectiveMeasures().get(colId);
                key = uniqueMeasureName(measure, ccMap);
                System.out.println(key);
            }
            if (uniqueRecItemMap.containsKey(key)) {
                colOrderInDB.add(-1 * uniqueRecItemMap.get(key).getId());
            } else {
                colOrderInDB.add(colId);
            }
        });
        return colOrderInDB;
    }

    private Map<String, ComputedColumnDesc> getCcOnModels(NDataModel model) {
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        model.getComputedColumnDescs().forEach(cc -> {
            String aliasDotName = cc.getTableAlias() + "." + cc.getColumnName();
            ccMap.putIfAbsent(aliasDotName, cc);
        });
        return ccMap;
    }

    private String uniqueMeasureName(NDataModel.Measure measure, Map<String, ComputedColumnDesc> ccMap) {
        Set<String> paramNames = Sets.newHashSet();
        List<ParameterDesc> parameters = measure.getFunction().getParameters();
        parameters.forEach(param -> {
            if (param.getColRef() == null) {
                paramNames.add(String.valueOf(Integer.MAX_VALUE));
                return;
            }
            ColumnDesc column = param.getColRef().getColumnDesc();
            if (column.isComputedColumn()) {
                paramNames.add(ccMap.get(column.getIdentity()).getUuid());
            } else {
                String tableAlias = param.getColRef().getTableRef().getAlias();
                String columnID = param.getColRef().getColumnDesc().getId();
                paramNames.add(tableAlias + "$" + columnID);
            }
        });
        return String.format("%s__%s", measure.getFunction().getExpression(), String.join("__", paramNames));
    }

    private List<RawRecItem> transferToMeasureRecItems(AbstractSemiContextV2 semiContextV2) {
        val mgr = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), semiContextV2.getProject());
        Map<String, RawRecItem> uniqueRecItemMap = mgr.listAll();
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getMeasureRecItemMap().forEach((name, measureItem) -> {
                RawRecItem item;
                if (uniqueRecItemMap.containsKey(name)) {
                    item = uniqueRecItemMap.get(name);
                    item.setUpdateTime(System.currentTimeMillis());
                } else {
                    item = new RawRecItem(semiContextV2.getProject(), //
                            modelContext.getTargetModel().getUuid(), //
                            modelContext.getTargetModel().getSemanticVersion(), //
                            RawRecItem.RawRecType.MEASURE);
                    item.setUniqueFlag(name);
                    item.setState(RawRecItem.RawRecState.INITIAL);
                    item.setCreateTime(measureItem.getCreateTime());
                    item.setUpdateTime(measureItem.getCreateTime());
                    item.setRecEntity(measureItem);
                    String[] params = name.split("__");
                    int[] dependIDs = new int[params.length - 1];
                    for (int i = 1; i < params.length; i++) {
                        dependIDs[i - 1] = uniqueRecItemMap.containsKey(params[i]) //
                                ? -1 * uniqueRecItemMap.get(params[i]).getId()
                                : Integer.parseInt(params[i].split("\\$")[1]);

                    }
                    item.setDependIDs(dependIDs);
                }
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToDimensionRecItems(AbstractSemiContextV2 semiContextV2) {
        val rcMgr = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), semiContextV2.getProject());
        Map<String, RawRecItem> uniqueRecItemMap = rcMgr.listAll();
        ArrayList<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getDimensionRecItemMap().forEach((name, dimItem) -> {
                RawRecItem item;
                if (uniqueRecItemMap.containsKey(name)) {
                    item = uniqueRecItemMap.get(name);
                    item.setUpdateTime(System.currentTimeMillis());
                } else {
                    item = new RawRecItem(semiContextV2.getProject(), //
                            modelContext.getTargetModel().getUuid(), //
                            modelContext.getTargetModel().getSemanticVersion(), //
                            RawRecItem.RawRecType.DIMENSION);
                    item.setUniqueFlag(name);
                    item.setCreateTime(dimItem.getCreateTime());
                    item.setUpdateTime(dimItem.getCreateTime());
                    item.setState(RawRecItem.RawRecState.INITIAL);
                    item.setRecEntity(dimItem);
                    String col = name.split("__")[1];
                    if (uniqueRecItemMap.containsKey(col)) {
                        item.setDependIDs(new int[] { -1 * uniqueRecItemMap.get(name).getId() });
                    } else {
                        item.setDependIDs(new int[] { dimItem.getColumn().getId() });
                    }
                }
                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private List<RawRecItem> transferToCCRawRecItem(AbstractSemiContextV2 semiContextV2) {
        val rcMgr = RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), semiContextV2.getProject());
        Map<String, RawRecItem> uniqueRecItemMap = rcMgr.listAll();
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        for (AbstractContext.NModelContext modelContext : semiContextV2.getModelContexts()) {
            modelContext.getCcRecItemMap().forEach((innerExp, ccItem) -> {
                RawRecItem item;
                if (uniqueRecItemMap.containsKey(innerExp)) {
                    item = uniqueRecItemMap.get(innerExp);
                    item.setUpdateTime(System.currentTimeMillis());
                } else {
                    item = new RawRecItem(semiContextV2.getProject(), //
                            modelContext.getTargetModel().getUuid(), //
                            modelContext.getTargetModel().getSemanticVersion(), //
                            RawRecItem.RawRecType.COMPUTED_COLUMN);
                    item.setCreateTime(ccItem.getCreateTime());
                    item.setUpdateTime(ccItem.getCreateTime());
                    item.setUniqueFlag(innerExp);
                    item.setRecEntity(ccItem);
                    item.setState(RawRecItem.RawRecState.INITIAL);
                    item.setDependIDs(new int[] { 1 });// todo
                }

                rawRecItems.add(item);
            });
        }
        return rawRecItems;
    }

    private void saveCCRawRecItems(List<RawRecItem> ccRawRecItems, String project) {
        RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), project).saveOrUpdate(ccRawRecItems);
    }

    private void saveDimensionAndMeasure(List<RawRecItem> dimensionRecItems, List<RawRecItem> measureRecItems,
            String project) {
        List<RawRecItem> recItems = Lists.newArrayList();
        recItems.addAll(dimensionRecItems);
        recItems.addAll(measureRecItems);
        RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), project).saveOrUpdate(recItems);

    }

    private void saveLayoutRawRecItems(List<RawRecItem> layoutRecItems, String project) {
        RawRecManager.getInstance(KylinConfig.getInstanceFromEnv(), project).saveOrUpdate(layoutRecItems);
    }

    private void updateRecommendationV2(String project, String id, List<Integer> rawIds) {
        OptimizeRecommendationManagerV2 managerV2 = OptimizeRecommendationManagerV2
                .getInstance(KylinConfig.getInstanceFromEnv(), project);
        managerV2.createOrUpdate(id, rawIds);
    }

}
