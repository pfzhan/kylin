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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.AbstractSemiContext;
import io.kyligence.kap.smart.ModelCreateContext;
import io.kyligence.kap.smart.ModelReuseContext;
import io.kyligence.kap.smart.SmartContext;
import lombok.var;

public class AccelerationContextUtil {

    private AccelerationContextUtil() {
    }

    public static AbstractContext newSmartContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        KylinConfigExt config = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig();
        return new SmartContext(config, project, sqlArray);
    }

    public static AbstractSemiContext newModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        KylinConfigExt config = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig();
        ModelReuseContext context = new ModelReuseContext(config, project, sqlArray);
        context.getExtraMeta().setOnlineModelIds(getOnlineModelIds(project));
        return context;
    }

    private static Set<String> getOnlineModelIds(String project) {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).listOnlineDataModels().stream()
                .map(RootPersistentEntity::getUuid).collect(Collectors.toSet());
    }

    public static AbstractSemiContext newModelCreateContext(KylinConfig kylinConfig, String project,
            String[] sqlArray) {
        return new ModelCreateContext(kylinConfig, project, sqlArray) {

            @Override
            public void saveMetadata() {
                saveModel();
                saveIndexPlan();
            }

            void saveModel() {
                NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        getProject());
                for (ModelContext modelCtx : getModelContexts()) {
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
                for (ModelContext modelContext : getModelContexts()) {
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
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataflowManager dfManager = NDataflowManager.getInstance(kylinConfig, context.getProject());
            NDataModel model = ctx.getTargetModel();
            if (model == null || dfManager.getDataflow(model.getId()) == null) {
                return;
            }
            dfManager.updateDataflowStatus(model.getId(), RealizationStatusEnum.ONLINE);
        });
    }
}
