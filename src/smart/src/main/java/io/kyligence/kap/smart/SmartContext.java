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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.ExpandableMeasureUtil;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.smart.model.ModelTree;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import lombok.Getter;

@Getter
public class SmartContext extends AbstractContext {

    private final ExpandableMeasureUtil expandableMeasureUtil = new ExpandableMeasureUtil((model, ccDesc) -> {
        String ccExpression = KapQueryUtil.massageComputedColumn(model, model.getProject(), ccDesc, null);
        ccDesc.setInnerExpression(ccExpression);
        ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
    });

    public SmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
        this.canCreateNewModel = true;
    }

    @Override
    public ModelContext createModelContext(ModelTree modelTree) {
        return new ModelContext(this, modelTree);
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        return NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getIndexPlan(modelId);
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).listAllModels().stream()
                .filter(model -> !model.isBroken()).collect(Collectors.toList());

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
        for (ModelContext modelCtx : getModelContexts()) {
            if (modelCtx.skipSavingMetadata()) {
                continue;
            }
            NDataModel model = modelCtx.getTargetModel();
            NDataModel updated;
            if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
                updated = dataModelManager.updateDataModelDesc(model);
            } else {
                updated = dataModelManager.createDataModelDesc(model, model.getOwner());
            }

            // expand measures
            expandableMeasureUtil.deleteExpandableMeasureInternalMeasures(updated);
            expandableMeasureUtil.expandExpandableMeasure(updated);
            updated = dataModelManager.updateDataModelDesc(updated);

            // update and expand index plan as well
            IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
            ExpandableMeasureUtil.expandRuleBasedIndex(indexPlan.getRuleBasedIndex(), updated);
            ExpandableMeasureUtil.expandIndexPlanIndexes(indexPlan, updated);
        }
    }

    private void saveIndexPlan() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
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

    @Override
    public ChainedProposer createProposers() {
        ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelSelectProposer(this), //
                new ModelOptProposer(this), //
                new ModelInfoAdjustProposer(this), //
                new ModelRenameProposer(this), //
                new IndexPlanSelectProposer(this), //
                new IndexPlanOptProposer(this), //
                new IndexPlanShrinkProposer(this) //
        );
        return new ChainedProposer(this, proposers);
    }
}
