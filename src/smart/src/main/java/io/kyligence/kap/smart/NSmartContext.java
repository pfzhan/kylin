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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.Getter;
import lombok.val;

@Getter
public class NSmartContext extends AbstractContext {

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
    }

    /**
     * Erase the layout in accelerate info map
     * @param layout the layout need erase
     * @return corresponding sqlPatterns of this layout
     */
    public Set<String> eraseLayoutInAccelerateInfo(LayoutEntity layout) {

        Preconditions.checkNotNull(layout);
        Set<String> sqlPatterns = Sets.newHashSet();
        for (val entry : getAccelerateInfoMap().entrySet()) {

            val relatedLayouts = entry.getValue().getRelatedLayouts();
            val iterator = relatedLayouts.iterator();
            while (iterator.hasNext()) {

                // only when layoutId, cubePlanId, modelId and semanticVersion consist with queryLayoutRelation would do erase
                final AccelerateInfo.QueryLayoutRelation next = iterator.next();
                if (next.consistent(layout)) {
                    Preconditions.checkState(entry.getKey().equalsIgnoreCase(next.getSql())); // must equal, otherwise error
                    iterator.remove();
                    sqlPatterns.add(entry.getKey());
                }
            }
        }
        return sqlPatterns;
    }

    /**
     * Rebuild accelerationInfoMap by relations between favorite query and layout from database
     * @param favoriteQueryRealizations serialized relations between layout and favorite query
     */
    public void reBuildAccelerationInfoMap(Set<FavoriteQueryRealization> favoriteQueryRealizations) {
        final Set<String> sqlPatternsSet = new HashSet<>(Lists.newArrayList(getSqlArray()));
        for (FavoriteQueryRealization fqRealization : favoriteQueryRealizations) {
            final String sqlPattern = fqRealization.getSqlPattern();
            if (!sqlPatternsSet.contains(sqlPattern))
                continue;
            if (!getAccelerateInfoMap().containsKey(sqlPattern)) {
                getAccelerateInfoMap().put(sqlPattern, new AccelerateInfo());
            }

            if (getAccelerateInfoMap().containsKey(sqlPattern)) {
                val queryRelatedLayouts = getAccelerateInfoMap().get(sqlPattern).getRelatedLayouts();
                String modelId = fqRealization.getModelId();
                long layoutId = fqRealization.getLayoutId();
                int semanticVersion = fqRealization.getSemanticVersion();
                val queryLayoutRelation = new AccelerateInfo.QueryLayoutRelation(sqlPattern, modelId, layoutId,
                        semanticVersion);
                queryRelatedLayouts.add(queryLayoutRelation);
            }
        }
    }

    public NModelContext createModelContext(ModelTree modelTree) {
        return new NModelContext(this, modelTree);
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        return NIndexPlanManager.getInstance(getKylinConfig(), getProject()).getIndexPlan(modelId);
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return NDataflowManager.getInstance(getKylinConfig(), getProject())
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
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getKylinConfig(), getProject());
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
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getKylinConfig(), getProject());
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getKylinConfig(), getProject());
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
    public ChainedProposer createChainedProposer() {
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
}
