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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.smart.common.AccelerateInfo;

public abstract class NAbstractProposer {

    protected static Logger logger = LoggerFactory.getLogger(NAbstractProposer.class);

    final NSmartContext smartContext;
    final KylinConfig kylinConfig;

    final String project;

    public NAbstractProposer(NSmartContext smartContext) {
        this.smartContext = smartContext;
        this.kylinConfig = smartContext.getKylinConfig();
        this.project = smartContext.getProject();
    }

    List<NDataModel> getOriginModels() {
        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        List<NDataModel> onlineModels = NDataflowManager.getInstance(kylinConfig, project)
                .listDataModelsByStatus(RealizationStatusEnum.ONLINE);
        if (projectInstance.isSemiAutoMode()) {
            return genRecommendationEnhancedModels(onlineModels);
        } else {
            return onlineModels;
        }
    }

    IndexPlan getOriginIndexPlan(String modelId) {
        return NProjectManager.getInstance(kylinConfig).getProject(project).isSemiAutoMode()
                ? OptimizeRecommendationManager.getInstance(kylinConfig, project).applyIndexPlan(modelId)
                : NIndexPlanManager.getInstance(kylinConfig, project).getIndexPlan(modelId);

    }

    private List<NDataModel> genRecommendationEnhancedModels(List<NDataModel> models) {
        List<NDataModel> enhancedDataModel = Lists.newArrayListWithCapacity(models.size());
        OptimizeRecommendationManager recommendMgr = OptimizeRecommendationManager.getInstance(kylinConfig, project);
        for (NDataModel model : models) {
            OptimizeRecommendation optimizeRecommendation = recommendMgr.getOptimizeRecommendation(model.getUuid());
            enhancedDataModel.add(recommendMgr.apply(model, optimizeRecommendation));
        }
        return enhancedDataModel;
    }

    void recordException(NSmartContext.NModelContext modelCtx, Exception e) {
        modelCtx.getModelTree().getOlapContexts().forEach(olapCtx -> {
            String sql = olapCtx.sql;
            final AccelerateInfo accelerateInfo = smartContext.getAccelerateInfoMap().get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            accelerateInfo.setFailedCause(e);
        });
    }

    abstract void propose();
}
