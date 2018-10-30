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

package io.kyligence.kap.smart;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NSmartMaster {

    private static final Logger logger = LoggerFactory.getLogger(NSmartMaster.class);

    private static final String MODEL_NAME_PREFIX = "AUTO_MODEL_";

    private NSmartContext context;
    private NProposerProvider proposerProvider;
    private NDataModelManager dataModelManager;

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls) {
        this.context = new NSmartContext(kylinConfig, project, sqls);
        this.proposerProvider = NProposerProvider.create(context);
        this.dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
    }

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls, String draftVersion) {
        this.context = new NSmartContext(kylinConfig, project, sqls, draftVersion);
        this.proposerProvider = NProposerProvider.create(context);
        this.dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
    }

    public NSmartContext getContext() {
        return context;
    }

    public void analyzeSQLs() {
        proposerProvider.getSQLAnalysisProposer().propose();
    }

    public void selectModel() {
        proposerProvider.getModelSelectProposer().propose();
    }

    public void optimizeModel() {
        proposerProvider.getModelOptProposer().propose();
    }

    public void selectCubePlan() {
        proposerProvider.getCubePlanSelectProposer().propose();
    }

    public void optimizeCubePlan() {
        proposerProvider.getCubePlanOptProposer().propose();
    }

    public void shrinkCubePlan() {
        proposerProvider.getCubePlanShrinkProposer().propose();
    }

    public void shrinkModel() {
        proposerProvider.getModelShrinkProposer().propose();
    }

    public void renameModel() {
        List<NDataModel> modelList = dataModelManager.listModels();
        Set<String> usedNames = Sets.newHashSet();
        if (modelList != null) {
            for (NDataModel model : modelList) {
                usedNames.add(model.getAlias());
            }
        }
        List<NSmartContext.NModelContext> modelContexts = context.getModelContexts();
        for (NSmartContext.NModelContext modelCtx : modelContexts) {
            NDataModel originalModel = modelCtx.getOrigModel();
            NDataModel targetModel = modelCtx.getTargetModel();
            if (originalModel == null) {
                String rootTableAlias = targetModel.getRootFactTable().getAlias();
                String modelName = getModelName(MODEL_NAME_PREFIX + rootTableAlias, usedNames);
                targetModel.setAlias(modelName);
            } else {
                targetModel.setAlias(originalModel.getAlias());
            }
        }
    }

    public void refreshCubePlan() {
        proposerProvider.getCubePlanRefreshProposer().propose();
    }

    public void refreshCubePlanWithRetry() throws IOException {
        int maxRetry = context.getSmartConfig().getProposeRetryMax();
        for (int i = 0; i <= maxRetry; i++) {
            try {
                selectModelAndCubePlan();
                refreshCubePlan();
                saveModel();
                saveCubePlan();
                logger.debug("save successfully after refresh");
                return;
            } catch (IllegalStateException e) {
                logger.warn("save error after refresh, have retried " + i + " times", e);
            }
        }

        throw new IllegalStateException("refreshCubePlanWithRetry exceed max retry count: " + maxRetry);
    }

    public void selectModelAndCubePlan() {
        analyzeSQLs();
        selectModel();
        selectCubePlan();
    }

    public void runAll() throws IOException {
        analyzeSQLs();
        selectModel();
        optimizeModel();
        renameModel();
        saveModel();

        selectCubePlan();
        optimizeCubePlan();
        saveCubePlan();
    }

    public List<NSmartContext.NModelContext> getModelContext(boolean isOptimize) throws IOException {
        if (isOptimize) {
            runAll();
        } else {
            analyzeSQLs();
            selectModel();
            selectCubePlan();
        }
        return getContext().getModelContexts();
    }

    public void saveCubePlan() throws IOException {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(context.getKylinConfig(), context.getProject());
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(context.getKylinConfig(), context.getProject());
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            NCubePlan cubePlan = modelCtx.getTargetCubePlan();
            if (cubePlanManager.getCubePlan(cubePlan.getName()) == null) {
                cubePlanManager.createCubePlan(cubePlan);
                dataflowManager.createDataflow(cubePlan.getName(), context.getProject(), cubePlan, null);
                continue;
            }

            cubePlan = cubePlanManager.updateCubePlan(cubePlan);

            NDataflow df = dataflowManager.getDataflow(cubePlan.getName());
            NDataflowUpdate update = new NDataflowUpdate(df.getName());
            List<NDataCuboid> toAddCuboids = Lists.newArrayList();

            for (NDataSegment seg : df.getSegments()) {
                Map<Long, NDataCuboid> cuboidMap = seg.getCuboidsMap();

                cubePlan.getAllCuboids().forEach(desc -> desc.getLayouts().forEach(layout -> {
                    if (!cuboidMap.containsKey(layout.getId())) {
                        toAddCuboids.add(NDataCuboid.newDataCuboid(df, seg.getId(), layout.getId()));
                    }
                }));
            }

            update.setToAddOrUpdateCuboids(toAddCuboids.toArray(new NDataCuboid[0]));
            dataflowManager.updateDataflow(update);
        }
    }

    public void saveModel() throws IOException {
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            NDataModel model = modelCtx.getTargetModel();
            if (dataModelManager.getDataModelDesc(model.getName()) != null) {
                dataModelManager.updateDataModelDesc(model);
            } else {
                dataModelManager.createDataModelDesc(model, null);
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private String getModelName(String seedModelName, Set<String> usedNames) {
        int suffix = 0;
        String targetName;
        do {
            if (suffix++ < 0) {
                throw new IllegalStateException("Potential infinite loop in getModelName().");
            }
            targetName = seedModelName + "_" + suffix;
        } while (usedNames.contains(targetName));
        usedNames.add(targetName);
        return targetName;
    }
}
