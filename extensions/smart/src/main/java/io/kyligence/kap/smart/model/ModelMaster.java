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

package io.kyligence.kap.smart.model;

import java.util.ArrayList;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.PartitionDesc;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.smart.model.proposer.ProposerProvider;

public class ModelMaster {

    private ModelContext context;
    private ProposerProvider proposerProvider;

    public ModelMaster(ModelContext ctx) {
        this.context = ctx;
        this.proposerProvider = ProposerProvider.create(this.context);
    }

    public ModelContext getContext() {
        return context;
    }

    public KapModel proposeInitialModel() {
        KapModel modelDesc = new KapModel();
        modelDesc.setName(context.getModelName() == null ? UUID.randomUUID().toString() : context.getModelName());
        modelDesc.setRootFactTableName(context.getRootTable().getIdentity());
        modelDesc.setDescription(StringUtils.EMPTY);
        modelDesc.setFilterCondition(StringUtils.EMPTY);
        modelDesc.setPartitionDesc(new PartitionDesc());
        modelDesc.setComputedColumnDescs(new ArrayList<ComputedColumnDesc>());
        return modelDesc;
    }

    public KapModel proposeJoins(KapModel modelDesc) {
        KapModel modelDescWithJoin = proposerProvider.getJoinProposer().propose(modelDesc);
        return modelDescWithJoin;
    }

    public KapModel proposeScope(KapModel modelDesc) {
        KapModel modelDescWithScope = proposerProvider.getScopeProposer().propose(modelDesc);
        return modelDescWithScope;
    }

    public KapModel proposeAll() {
        KapModel modelDesc = proposeInitialModel();
        KapModel modelDescWithJoin = proposeJoins(modelDesc);
        KapModel modelDescWithScope = proposeScope(modelDescWithJoin);
        return modelDescWithScope;
    }
}
