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
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.Getter;

@Getter
public class NSmartContext extends AbstractContext {

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
    }

    public NModelContext createModelContext(ModelTree modelTree) {
        return new NModelContext(this, modelTree);
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        return NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getIndexPlan(modelId);
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .listDataModelsByStatus(RealizationStatusEnum.ONLINE);

    }

    @Override
    public void changeModelMainType(NDataModel model) {
        model.setManagementType(ManagementType.TABLE_ORIENTED);
    }

    @Override
    public String getIdentifier() {
        return "Auto-Modeling";
    }

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
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
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

    @Override
    public ChainedProposer createTransactionProposers() {
        ImmutableList<NAbstractProposer> proposers = ImmutableList.of(//
                new NSQLAnalysisProposer(this), //
                new NModelSelectProposer(this), //
                new NModelOptProposer(this), //
                new NModelInfoAdjustProposer(this), //
                new NModelRenameProposer(this), //
                new NIndexPlanSelectProposer(this), //
                new NIndexPlanOptProposer(this), //
                new NIndexPlanShrinkProposer(this) //
        );
        return new ChainedProposer(this, proposers);
    }

    @Override
    public ChainedProposer createPreProcessProposers() {
        return new ChainedProposer(this, ImmutableList.of());
    }
}
