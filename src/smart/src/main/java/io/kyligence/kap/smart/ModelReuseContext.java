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

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.smart.common.AccelerateInfo;

public class ModelReuseContext extends AbstractSemiContext {

    public ModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        super(kylinConfig, project, sqlArray);
        this.partialMatch = getSmartConfig().getKylinConfig().isQueryMatchPartialInnerJoinModel();
        this.partialMatchNonEqui = getSmartConfig().getKylinConfig().partialMatchNonEquiJoins();
    }

    public ModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray, boolean canCreateNewModel) {
        this(kylinConfig, project, sqlArray);
        this.canCreateNewModel = canCreateNewModel;
    }

    @Override
    public ChainedProposer createProposers() {
        return new ChainedProposer(this, ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelSelectProposer(this), //
                new ViewModelSelectProposer(this), //
                new ModelOptProposer(this), //
                new ModelInfoAdjustProposer(this), //
                new ModelRenameProposer(this), //
                new IndexPlanSelectProposer(this), //
                new IndexPlanOptProposer(this), //
                new IndexPlanShrinkProposer(this) //
        ));
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
                    accelerateInfo.setPendingMsg(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG);
                });
            }
        });
    }
}
