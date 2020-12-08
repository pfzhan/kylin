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

package io.kyligence.kap.smart.util;

import static org.apache.kylin.common.util.AbstractKylinTestCase.getTestConfig;
import static org.apache.kylin.metadata.realization.RealizationStatusEnum.ONLINE;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiContextV2;
import io.kyligence.kap.smart.ModelCreateContextOfSemiV2;
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import io.kyligence.kap.smart.NSmartContext;
import lombok.val;
import lombok.var;

public class AccelerationContextUtil {

    private AccelerationContextUtil() {
    }

    public static AbstractContext newSmartContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        return new NSmartContext(kylinConfig, project, sqlArray);
    }

    public static AbstractSemiContextV2 newModelReuseContext(KylinConfig kylinConfig, String project,
            String[] sqlArray) {
        return new ModelReuseContextOfSemiV2(kylinConfig, project, sqlArray);
    }

    public static AbstractSemiContextV2 newModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray,
            boolean canCreateNewModel) {
        return new ModelReuseContextOfSemiV2(kylinConfig, project, sqlArray, canCreateNewModel);
    }

    public static AbstractSemiContextV2 newModelCreateContext(KylinConfig kylinConfig, String project,
            String[] sqlArray) {
        return new ModelCreateContextOfSemiV2(kylinConfig, project, sqlArray) {

            @Override
            public void saveMetadata() {
                saveModel();
                saveIndexPlan();
            }

            void saveModel() {
                NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        getProject());
                for (AbstractContext.NModelContext modelCtx : getModelContexts()) {
                    if (modelCtx.skipSavingMetadata()) {
                        continue;
                    }
                    NDataModel model = modelCtx.getTargetModel();
                    if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
                        dataModelManager.updateDataModelDesc(model);
                    } else {
                        dataModelManager.createDataModelDesc(model, model.getOwner());
                    }
                }
            }

            private void saveIndexPlan() {
                NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        getProject());
                NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        getProject());
                for (AbstractContext.NModelContext modelContext : getModelContexts()) {
                    if (modelContext.skipSavingMetadata()) {
                        continue;
                    }
                    IndexPlan indexPlan = modelContext.getTargetIndexPlan();
                    if (indexPlanManager.getIndexPlan(indexPlan.getUuid()) == null) {
                        indexPlanManager.createIndexPlan(indexPlan);
                        dataflowManager.createDataflow(indexPlan, indexPlan.getModel().getOwner());
                    } else {
                        indexPlanManager.updateIndexPlan(indexPlan);
                    }
                }
            }
        };
    }

    public static void transferProjectToSemiAutoMode(KylinConfig kylinConfig, String project) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    public static void transferProjectToPureExpertMode(KylinConfig kylinConfig, String project) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "false");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    public static void onlineModel(AbstractContext context) {
        if (context == null || context.getModelContexts() == null) {
            return;
        }
        context.getModelContexts().forEach(ctx -> {
            val dfManager = NDataflowManager.getInstance(getTestConfig(), context.getProject());
            val model = ctx.getTargetModel();
            if (model == null || dfManager.getDataflow(model.getId()) == null) {
                return;
            }
            dfManager.updateDataflow(model.getId(), copyForWrite -> copyForWrite.setStatus(ONLINE));
        });
    }
}
