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

import java.util.Map;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.Getter;

public class ModelReuseContextOfSemiV2 extends AbstractSemiContextV2 {
    @Getter
    private boolean canCreateNewModel;

    public ModelReuseContextOfSemiV2(KylinConfig kylinConfig, String project, String[] sqlArray) {
        super(kylinConfig, project, sqlArray);
        this.partialMatch = kylinConfig.isQueryMatchPartialInnerJoinModel();
    }

    public ModelReuseContextOfSemiV2(KylinConfig kylinConfig, String project, String[] sqlArray,
            boolean canCreateNewModel) {
        this(kylinConfig, project, sqlArray);
        this.canCreateNewModel = canCreateNewModel;
    }

    @Override
    public ChainedProposer createTransactionProposers() {
        return new ChainedProposer(this, ImmutableList.of(//
                new NSQLAnalysisProposer(this), //
                new NModelSelectProposer(this), //
                new NModelOptProposer(this), //
                new NModelInfoAdjustProposer(this), //
                new NModelRenameProposer(this), //
                new NIndexPlanSelectProposer(this), //
                new NIndexPlanOptProposer(this), //
                new NIndexPlanShrinkProposer(this) //
        ));
    }

    @Override
    public ChainedProposer createPreProcessProposers() {
        return new ChainedProposer(this, ImmutableList.of());
    }

    @Override
    public void saveMetadata() {
        // do nothing
    }

    @Override
    public Map<String, String> getInnerExpToUniqueFlag() {
        Map<String, RawRecItem> recItemMap = RawRecManager.getInstance(getProject()).listAll();
        Map<String, String> ccInnerExpToUniqueFlag = Maps.newHashMap();
        recItemMap.forEach((k, v) -> {
            if (v.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN) {
                final CCRecItemV2 recEntity = (CCRecItemV2) v.getRecEntity();
                ccInnerExpToUniqueFlag.put(recEntity.getCc().getInnerExpression(), k);
            }
        });
        return ccInnerExpToUniqueFlag;
    }

    @Override
    public void handleExceptionAfterModelSelect() {
        if (isCanCreateNewModel()) {
            return;
        }

        getModelContexts().forEach(modelCtx -> {
            if (modelCtx.isTargetModelMissing()) {
                modelCtx.getModelTree().getOlapContexts().forEach(olapContext -> {
                    AccelerateInfo accelerateInfo = getAccelerateInfoMap().get(olapContext.sql);
                    accelerateInfo.setPendingMsg(NModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG);
                });
            }
        });
    }
}
