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

package io.kyligence.kap.metadata.model;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.util.CubeUtils;

public class NModelMaster {
    private NSmartContext.NModelContext context;
    private NProposerProvider proposerProvider;

    public NModelMaster(NSmartContext.NModelContext ctx) {
        this.context = ctx;
        this.proposerProvider = NProposerProvider.create(this.context);
    }

    public NSmartContext.NModelContext getContext() {
        return context;
    }

    public NDataModel proposeInitialModel() {
        NDataModel modelDesc = new NDataModel();
        modelDesc.updateRandomUuid();
        modelDesc.setName(modelDesc.getUuid());
        modelDesc.setRootFactTableName(context.getModelTree().getRootFactTable().getIdentity());
        modelDesc.setDescription(StringUtils.EMPTY);
        modelDesc.setFilterCondition(StringUtils.EMPTY);
        modelDesc.setPartitionDesc(new PartitionDesc());
        modelDesc.setComputedColumnDescs(new ArrayList<ComputedColumnDesc>());

        FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(modelDesc);
        NDataModel.Measure countStarMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", NDataModel.MEASURE_ID_BASE);
        modelDesc.setAllMeasures(Lists.newArrayList(countStarMeasure));
        return modelDesc;
    }

    public NDataModel proposeJoins(NDataModel modelDesc) {
        return proposerProvider.getJoinProposer().propose(modelDesc);
    }

    public NDataModel proposeScope(NDataModel modelDesc) {
        return proposerProvider.getScopeProposer().propose(modelDesc);
    }
}
