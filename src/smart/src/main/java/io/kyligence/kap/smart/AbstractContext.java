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
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class AbstractContext {

    private final SmartConfig smartConfig;
    private final String project;
    private final String[] sqlArray;
    private final ChainedProposer preProcessProposers;
    private final ChainedProposer processProposers;

    @Setter
    private List<AbstractContext.NModelContext> modelContexts;
    private final Map<String, AccelerateInfo> accelerateInfoMap = Maps.newHashMap();
    @Getter
    private final Map<NDataModel, OptimizeRecommendation> recommendationMap = Maps.newHashMap();
    @Getter(lazy = true)
    private final Map<String, RawRecItem> recItemMap = Maps.newHashMap();

    @Setter
    private boolean skipEvaluateCC;
    protected boolean partialMatch;

    protected AbstractContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        this.smartConfig = SmartConfig.wrap(kylinConfig);
        this.project = project;
        this.sqlArray = sqlArray;
        this.preProcessProposers = createPreProcessProposers();
        this.processProposers = createTransactionProposers();
        this.partialMatch = false;
    }

    public NModelContext createModelContext(ModelTree modelTree) {
        return new NModelContext(this, modelTree);
    }

    public abstract IndexPlan getOriginIndexPlan(String modelId);

    public abstract List<NDataModel> getOriginModels();

    public abstract void changeModelMainType(NDataModel model);

    public abstract ChainedProposer createTransactionProposers();

    public abstract ChainedProposer createPreProcessProposers();

    public abstract void saveMetadata();

    public abstract String getIdentifier();

    public void recordException(AbstractContext.NModelContext modelCtx, Exception e) {
        modelCtx.getModelTree().getOlapContexts().forEach(olapCtx -> {
            String sql = olapCtx.sql;
            final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            accelerateInfo.setFailedCause(e);
        });
    }

    public boolean needCollectRecommendations() {
        return this instanceof AbstractSemiContextV2;
    }

    public void handleExceptionAfterModelSelect() {
        // default do nothing 
    }

    @Getter
    public static class NModelContext {
        @Setter
        private ModelTree modelTree; // query

        @Setter
        private NDataModel targetModel; // output model
        @Setter
        private NDataModel originModel; // used when update existing models

        @Setter
        private IndexPlan targetIndexPlan;
        @Setter
        private IndexPlan originIndexPlan;

        @Setter
        private Map<String, CCRecItemV2> ccRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, DimensionRecItemV2> dimensionRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, MeasureRecItemV2> measureRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, LayoutRecItemV2> indexRexItemMap = Maps.newHashMap();

        @Setter
        private boolean snapshotSelected;

        private final AbstractContext proposeContext;
        private final Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();
        @Setter
        private boolean needUpdateCC = false;
        @Getter(lazy = true)
        private final Map<String, String> uniqueContentToFlag = loadUniqueContentToFlag();

        private Map<String, String> loadUniqueContentToFlag() {
            Map<String, String> result = Maps.newHashMap();
            if (!(getProposeContext() instanceof AbstractSemiContextV2) || getTargetModel() == null) {
                return result;
            }

            String modelId = getTargetModel().getUuid();
            getProposeContext().getRecItemMap().forEach((uniqueFlag, item) -> {
                if (item.getModelID().equalsIgnoreCase(modelId)) {
                    result.put(item.getRecEntity().getUniqueContent(), uniqueFlag);
                }
            });
            return result;
        }

        public NModelContext(AbstractContext proposeContext, ModelTree modelTree) {
            this.proposeContext = proposeContext;
            this.modelTree = modelTree;
        }

        public boolean isTargetModelMissing() {
            return targetModel == null;
        }

        public boolean isProposedIndexesEmpty() {
            // we can not modify rule_based_indexes
            return targetIndexPlan == null || CollectionUtils.isEmpty(targetIndexPlan.getIndexes());
        }

        public boolean skipSavingMetadata() {
            return isTargetModelMissing() || isProposedIndexesEmpty() || snapshotSelected;
        }

        /**
         * Only for Semi-Auto
         */
        public void gatherLayoutRecItem(LayoutEntity layout) {
            if (!getProposeContext().needCollectRecommendations()) {
                return;
            }
            LayoutRecItemV2 item = new LayoutRecItemV2();
            item.setLayout(layout);
            item.setCreateTime(System.currentTimeMillis());
            item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
            item.setUuid(UUID.randomUUID().toString());
            getIndexRexItemMap().put(layout.genUniqueFlag(), item);
        }
    }
}
