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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.AbstractContext;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractModelProposer {

    protected AbstractContext.ModelContext modelContext;
    final String project;
    final NDataModelManager dataModelManager;

    AbstractModelProposer(AbstractContext.ModelContext modelCtx) {
        this.modelContext = modelCtx;

        project = modelCtx.getProposeContext().getProject();

        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    public AbstractContext.ModelContext getModelContext() {
        return modelContext;
    }

    public NDataModel propose(NDataModel origModel) {
        NDataModel modelDesc = dataModelManager.copyForWrite(origModel);
        initModel(modelDesc);
        execute(modelDesc);
        initModel(modelDesc);
        return modelDesc;
    }

    void initModel(NDataModel modelDesc) {
        modelDesc.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
    }

    boolean isValidOlapContext(OLAPContext context) {
        val accelerateInfo = modelContext.getProposeContext().getAccelerateInfoMap().get(context.sql);
        return accelerateInfo != null && !accelerateInfo.isNotSucceed();
    }

    protected abstract void execute(NDataModel modelDesc);
}
