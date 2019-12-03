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

package io.kyligence.kap.newten.auto;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationVerifier;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import lombok.val;
import lombok.var;

@Slf4j
public class SemiAutoTestBase extends NSuggestTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();

        val optManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
        optManager.listAllOptimizeRecommendationIds().forEach(optManager::dropOptimizeRecommendation);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        dataflowManager.listAllDataflows().stream().map(RootPersistentEntity::getId)
                .forEach(dataflowManager::dropDataflow);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexPlanManager.listAllIndexPlans().forEach(indexPlanManager::dropIndexPlan);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModelIds().forEach(modelManager::dropModel);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "default";
    }

    protected void prepareModels(String project, NSuggestTestBase.TestScenario... testScenarios) throws IOException {
        val prjManager = NProjectManager.getInstance(getTestConfig());
        prjManager.updateProject(getProject(), copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kap.metadata.semi-automatic-mode", "false");
            copyForWrite.setOverrideKylinProps(properties);
        });
        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val originModelIds = modelManager.listAllModelIds();
        val originModels = modelManager.listAllModels().stream()
                .map(model -> JsonUtil.deepCopyQuietly(model, NDataModel.class))
                .collect(Collectors.toMap(NDataModel::getId, m -> m));
        val originIndexPlans = indexPlanManager.listAllIndexPlans().stream()
                .filter(indexPlan -> originModelIds.contains(indexPlan.getId()))
                .map(indexPlan -> JsonUtil.deepCopyQuietly(indexPlan, IndexPlan.class))
                .collect(Collectors.toMap(IndexPlan::getId, i -> i));
        proposeWithSmartMaster(project, NSmartMaster::runAll, testScenarios);
        prjManager.updateProject(getProject(), copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kap.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
        emptyModelAndIndexPlan(project, originModelIds, originModels, originIndexPlans);
    }

    protected void prepareModels(String project, String modelNameFolder) throws IOException {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        var dir = new File(getFolder(modelNameFolder));
        Lists.newArrayList(Objects.requireNonNull(dir.listFiles())).forEach(file -> {
            var model = JsonUtil.readValueQuietly(file, NDataModel.class);
            model = modelManager.createDataModelDesc(model, "ADMIN");
            val indexPlan = new IndexPlan();
            indexPlan.setUuid(model.getUuid());
            indexPlan.setProject(project);
            indexPlanManager.createIndexPlan(indexPlan);
            dataflowManager.createDataflow(indexPlan, "ADMIN");
        });
    }

    private void emptyModelAndIndexPlan(String project, Set<String> originModelIds,
            Map<String, NDataModel> originModels, Map<String, IndexPlan> originIndexPlans) {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val newModelIds = Sets.difference(modelManager.listAllModelIds(), originModelIds);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.listAllDataflows().stream().map(RootPersistentEntity::getId).filter(newModelIds::contains)
                .forEach(dataflowManager::dropDataflow);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        indexPlanManager.listAllIndexPlans().stream().filter(indexPlan -> newModelIds.contains(indexPlan.getId()))
                .forEach(indexPlanManager::dropIndexPlan);
        modelManager.listAllModelIds().forEach(modelId -> modelManager.updateDataModel(modelId, copyForWrite -> {
            if (newModelIds.contains(modelId)) {
                val removeCCs = copyForWrite.getComputedColumnDescs().stream().map(ComputedColumnDesc::getFullName)
                        .collect(Collectors.toSet());
                copyForWrite.setComputedColumnDescs(Lists.newArrayList());
                copyForWrite.setAllNamedColumns(copyForWrite.getAllNamedColumns().stream()
                        .filter(n -> !removeCCs.contains(n.getAliasDotColumn()))
                        .peek(n -> n.setStatus(NDataModel.ColumnStatus.EXIST))
                        .sorted(Comparator.comparing(NDataModel.NamedColumn::getId)).collect(Collectors.toList()));
                for (int i = 0; i < copyForWrite.getAllNamedColumns().size(); i++) {
                    copyForWrite.getAllNamedColumns().get(i).setId(i);
                }
                copyForWrite
                        .setAllMeasures(copyForWrite.getAllMeasures().stream().limit(1).collect(Collectors.toList()));
                val indexPlan = new IndexPlan();
                indexPlan.setUuid(modelId);
                indexPlan.setProject(project);
                indexPlanManager.createIndexPlan(indexPlan);
                dataflowManager.createDataflow(indexPlan, "ADMIN");
            } else {
                if (originIndexPlans.get(modelId).getMvcc() < indexPlanManager.getIndexPlan(modelId).getMvcc()) {
                    originIndexPlans.get(modelId).setMvcc(indexPlanManager.getIndexPlan(modelId).getMvcc());
                    indexPlanManager.updateIndexPlan(originIndexPlans.get(modelId));
                }

                if (originModels.get(modelId).getMvcc() < modelManager.getDataModelDesc(modelId).getMvcc()) {
                    originModels.get(modelId).setMvcc(modelManager.getDataModelDesc(modelId).getMvcc());
                    modelManager.updateDataModelDesc(originModels.get(modelId));
                }
            }

        }));
    }

    @Override
    protected Map<String, RecAndQueryCompareUtil.CompareEntity> executeTestScenario(boolean recordFQ,
            TestScenario... testScenarios) throws Exception {

        long startTime = System.currentTimeMillis();
        final NSmartMaster smartMaster = proposeWithSmartMaster(getProject(), testScenarios);
        verifyAll();
        updateAccelerateInfoMap(smartMaster);
        final Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap = collectCompareEntity(smartMaster);
        log.debug("smart proposal cost {} ms", System.currentTimeMillis() - startTime);

        buildAndCompare(compareMap, testScenarios);

        startTime = System.currentTimeMillis();
        // 4. compare layout propose result and query cube result
        RecAndQueryCompareUtil.computeCompareRank(kylinConfig, getProject(), compareMap);
        // 5. check layout
        assertOrPrintCmpResult(compareMap.entrySet().stream()
                .filter(entry -> !entry.getValue().getLevel()
                        .equals(RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        log.debug("compare realization cost {} s", System.currentTimeMillis() - startTime);

        // 6. summary info
        val rankInfoMap = RecAndQueryCompareUtil.summarizeRankInfo(compareMap);
        StringBuilder sb = new StringBuilder();
        sb.append("All used queries: ").append(compareMap.size()).append('\n');
        rankInfoMap.forEach((key, value) -> sb.append(key).append(": ").append(value).append("\n"));
        System.out.println(sb);
        return compareMap;
    }

    private void updateAccelerateInfoMap(NSmartMaster smartMaster) {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val targetIndexPlanMap = smartMaster.getContext().getModelContexts().stream()
                .map(NSmartContext.NModelContext::getTargetIndexPlan)
                .collect(Collectors.toMap(RootPersistentEntity::getId, i -> i));
        val targetModelMap = smartMaster.getContext().getModelContexts().stream()
                .map(NSmartContext.NModelContext::getTargetModel)
                .collect(Collectors.toMap(RootPersistentEntity::getId, i -> i));
        smartMaster.getContext().getAccelerateInfoMap().forEach((sql, info) -> {
            info.getRelatedLayouts().forEach(r -> {
                val modelId = r.getModelId();
                val indexPlan = indexManager.getIndexPlan(modelId);
                val indexInSmart = targetIndexPlanMap.get(modelId);
                val modelInSmart = targetModelMap.get(modelId);
                r.setLayoutId(indexPlan
                        .getAllLayouts().stream().filter(l -> isMatch(l, indexInSmart.getCuboidLayout(r.getLayoutId()),
                                indexPlan.getModel(), modelInSmart))
                        .map(LayoutEntity::getId).findFirst().orElse(r.getLayoutId()));
            });
        });
    }

    private boolean isMatch(LayoutEntity real, LayoutEntity virtual, NDataModel realModel, NDataModel virtualModel) {
        val copy = JsonUtil.deepCopyQuietly(virtual, LayoutEntity.class);
        copy.setColOrder(translate(copy.getColOrder(), realModel, virtualModel));
        copy.setShardByColumns(translate(copy.getShardByColumns(), realModel, virtualModel));
        copy.setSortByColumns(translate(copy.getSortByColumns(), realModel, virtualModel));
        return real.equals(copy);
    }

    private List<Integer> translate(List<Integer> cols, NDataModel realModel, NDataModel virtualModel) {
        val realColsMap = realModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, NDataModel.NamedColumn::getId));
        val realMeasureMap = realModel.getAllMeasures().stream().filter(m -> !m.isTomb())
                .collect(Collectors.toMap(MeasureDesc::getName, NDataModel.Measure::getId));
        val virtualColsMap = virtualModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn));
        val virtualMeasureMap = virtualModel.getAllMeasures().stream().filter(m -> !m.isTomb())
                .collect(Collectors.toMap(NDataModel.Measure::getId, MeasureDesc::getName));
        return cols.stream().map(i -> {
            if (i < NDataModel.MEASURE_ID_BASE) {
                return realColsMap.get(virtualColsMap.get(i));
            } else {
                return realMeasureMap.get(virtualMeasureMap.get(i));
            }
        }).collect(Collectors.toList());
    }

    @Override
    protected NSmartMaster proposeWithSmartMaster(String project, TestScenario... testScenarios) throws IOException {
        return proposeWithSmartMaster(project, NSmartMaster::runOptRecommendation, testScenarios);
    }

    protected void verifyAll() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModels().stream().filter(m -> !m.isBroken()).forEach(m -> {
            val optimizeManager = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject());
            val recommendation = optimizeManager.getOptimizeRecommendation(m.getId());
            if (recommendation == null) {
                return;
            }
            new OptimizeRecommendationVerifier(getTestConfig(), getProject(), m.getId()).verifyAll();
        });
    }

    protected Map<String, RecAndQueryCompareUtil.CompareEntity> collectCompareEntity(NSmartMaster smartMaster) {
        Map<String, RecAndQueryCompareUtil.CompareEntity> map = Maps.newHashMap();
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((sql, accelerateInfo) -> {
            map.putIfAbsent(sql, new RecAndQueryCompareUtil.CompareEntity());
            final RecAndQueryCompareUtil.CompareEntity entity = map.get(sql);
            entity.setAccelerateInfo(accelerateInfo);
            entity.setAccelerateLayouts(RecAndQueryCompareUtil.writeQueryLayoutRelationAsString(kylinConfig,
                    getProject(), accelerateInfo.getRelatedLayouts()));
            entity.setSql(sql);
            entity.setLevel(RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY);
        });
        return map;
    }

    protected void buildAndCompare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            TestScenario... testScenarios) throws Exception {
        try {
            buildAllCubes(kylinConfig, getProject());
            compare(compareMap, testScenarios);
        } finally {
            FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        }
    }

    protected void compare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            TestScenario... testScenarios) {
        Arrays.stream(testScenarios)
                .forEach(testScenario -> compare(compareMap, testScenario, testScenario.queries, Lists.newArrayList()));
    }

    protected void compare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap, TestScenario testScenario,
            List<Pair<String, String>> validQueries, List<Pair<String, String>> invalidQueries) {
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        if (testScenario.isLimit()) {
            NExecAndComp.execLimitAndValidateNew(validQueries, getProject(), JoinType.DEFAULT.name(), compareMap);
        } else if (testScenario.isDynamicSql()) {
            NExecAndComp.execAndCompareDynamic(validQueries, getProject(), testScenario.getCompareLevel(),
                    testScenario.joinType.name(), compareMap);
        } else {
            NExecAndComp.execAndCompareNew(validQueries, getProject(), testScenario.getCompareLevel(),
                    testScenario.joinType.name(), compareMap);
            NExecAndComp.execAndFail(invalidQueries, getProject(), testScenario.joinType.name(), compareMap);

        }
    }
}
