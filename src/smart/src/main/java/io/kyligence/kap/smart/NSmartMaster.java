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

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.VersionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class NSmartMaster {

    private static final Logger logger = LoggerFactory.getLogger(NSmartMaster.class);

    private static final String MODEL_NAME_PREFIX = "AUTO_MODEL_";

    private NSmartContext context;
    private NProposerProvider proposerProvider;
    private NDataModelManager dataModelManager;
    FavoriteQueryManager favoriteQueryManager;
    private String project;

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls) {
        this.project = project;
        this.context = new NSmartContext(kylinConfig, project, sqls);
        this.proposerProvider = NProposerProvider.create(context);
        this.dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
        this.favoriteQueryManager = FavoriteQueryManager.getInstance(kylinConfig, project);
    }

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls, String draftVersion) {
        this.project = project;
        this.context = new NSmartContext(kylinConfig, project, sqls, draftVersion);
        this.proposerProvider = NProposerProvider.create(context);
        this.dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
        this.favoriteQueryManager = FavoriteQueryManager.getInstance(kylinConfig, project);
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
        List<NDataModel> modelList = dataModelManager.getDataModels();
        Set<String> usedNames = Sets.newHashSet();
        if (modelList != null) {
            for (NDataModel model : modelList) {
                usedNames.add(model.getAlias());
            }
        }
        List<NSmartContext.NModelContext> modelContexts = context.getModelContexts();
        for (NSmartContext.NModelContext modelCtx : modelContexts) {
            if (modelCtx.withoutTargetModel()) {
                continue;
            }

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

    public void refreshCubePlanWithRetry() {
        int maxRetry = context.getSmartConfig().getProposeRetryMax();
        for (int i = 0; i <= maxRetry; i++) {
            try {
                selectModelAndCubePlan();
                refreshCubePlan();
                saveCubePlan();
                saveAccelerateInfo();
                logger.debug("save successfully after refresh, {}", context.getDraftVersion());
                return;
            } catch (IllegalStateException | VersionConflictException e) {
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

    public void runAll() {
        runAllAndForContext(null);
    }

    public void runAllAndForContext(Consumer<NSmartContext> hook) {
        analyzeSQLs();
        selectModel();
        optimizeModel();
        renameModel();
        selectCubePlan();
        optimizeCubePlan();

        UnitOfWork.doInTransactionWithRetry(() -> {
            saveModel();
            saveCubePlan();
            saveAccelerateInfo();
            if (hook != null) {
                hook.accept(getContext());
            }
            return null;
        }, project);
    }

    public void saveCubePlan() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(context.getKylinConfig(), context.getProject());
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(context.getKylinConfig(), context.getProject());
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.withoutTargetModel()) {
                continue;
            }
            NCubePlan cubePlan = modelCtx.getTargetCubePlan();
            if (cubePlanManager.getCubePlan(cubePlan.getName()) == null) {
                cubePlanManager.createCubePlan(cubePlan);
                dataflowManager.createDataflow(cubePlan.getName(), cubePlan, cubePlan.getModel().getOwner());
                continue;
            }

            cubePlanManager.updateCubePlan(cubePlan);
        }
    }

    public void saveAccelerateInfo() {
        val accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((sqlPattern, accelerateInfo) -> {
            if (!accelerateInfo.isBlocked()) {
                List<FavoriteQueryRealization> favoriteQueryRealizations = Lists.newArrayList();
                for (val layout : accelerateInfo.getRelatedLayouts()) {
                    FavoriteQueryRealization realization = new FavoriteQueryRealization();
                    realization.setModelId(layout.getModelId());
                    realization.setCubePlanId(layout.getCubePlanId());
                    realization.setCuboidLayoutId(layout.getLayoutId());
                    favoriteQueryRealizations.add(realization);
                }

                favoriteQueryManager.resetRealizations(sqlPattern, favoriteQueryRealizations);
            }
        });
    }

    public void saveModel() {
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.withoutTargetModel()) {
                continue;
            }
            NDataModel model = modelCtx.getTargetModel();
            if (dataModelManager.getDataModelDesc(model.getName()) != null) {
                dataModelManager.updateDataModelDesc(model);
            } else {
                dataModelManager.createDataModelDesc(model, model.getOwner());
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
