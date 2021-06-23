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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;

public class ModelSelectContextOfSemiV2 extends AbstractSemiContextV2 {

    public ModelSelectContextOfSemiV2(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
        this.partialMatch = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig()
                .isQueryMatchPartialInnerJoinModel();
    }

    @Override
    public ChainedProposer createPreProcessProposers() {
        ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelSelectProposer(this));
        return new ChainedProposer(this, proposers);
    }

    @Override
    public ChainedProposer createTransactionProposers() {
        return new ChainedProposer(this, ImmutableList.of());
    }

    @Override
    public void saveMetadata() {
        // Just implement it
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        throw new NotImplementedException("Fetch origin indexes is forbidden in ModelSelectAIAugmentedContext!");
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).listAllModels().stream()
                .filter(model -> getExtraMeta().getOnlineModelIds().contains(model.getUuid()))
                .collect(Collectors.toList());

    }

    @Override
    public void changeModelMainType(NDataModel model) {
        throw new NotImplementedException("Modifying ModelMaintainType is forbidden in ModelSelectAIAugmentedContext");
    }
}
