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

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableList;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelCreateContext extends AbstractSemiContext {

    public ModelCreateContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        super(kylinConfig, project, sqlArray);
        this.canCreateNewModel = true;
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return Lists.newArrayList();
    }

    @Override
    public ChainedProposer createProposers() {
        ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                new SQLAnalysisProposer(this), //
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
