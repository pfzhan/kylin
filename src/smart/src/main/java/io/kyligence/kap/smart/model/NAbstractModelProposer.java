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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.NSmartContext;
import lombok.val;

public abstract class NAbstractModelProposer {

    static final Logger LOGGER = LoggerFactory.getLogger(NAbstractModelProposer.class);

    protected NSmartContext.NModelContext modelContext;
    final KylinConfig kylinConfig;
    final String project;

    NAbstractModelProposer(NSmartContext.NModelContext modelCtx) {
        this.modelContext = modelCtx;

        kylinConfig = modelCtx.getSmartContext().getKylinConfig();
        project = modelCtx.getSmartContext().getProject();
    }

    public NSmartContext.NModelContext getModelContext() {
        return modelContext;
    }

    public NDataModel propose(NDataModel origModel) {
        NDataModel modelDesc = NDataModel.getCopyOf(origModel);
        initModel(modelDesc);
        doPropose(modelDesc);
        initModel(modelDesc);
        return modelDesc;
    }

    void initModel(NDataModel modelDesc) {
        final NTableMetadataManager manager = NTableMetadataManager.getInstance(kylinConfig, project);
        modelDesc.init(kylinConfig, manager.getAllTablesMap(), Lists.newArrayList(), project);
    }

    protected abstract void doPropose(NDataModel modelDesc);

    boolean isValidOlapContext(OLAPContext context) {
        val accelerateInfo = modelContext.getSmartContext().getAccelerateInfoMap().get(context.sql);
        return accelerateInfo != null && !accelerateInfo.isBlocked();
    }
}
