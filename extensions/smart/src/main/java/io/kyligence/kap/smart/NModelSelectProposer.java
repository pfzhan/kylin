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

import org.apache.kylin.metadata.model.DataModelDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.model.ModelTree;

public class NModelSelectProposer extends NAbstractProposer {
    private static final Logger logger = LoggerFactory.getLogger(NModelSelectProposer.class);
    private final NDataModelManager modelManager;

    public NModelSelectProposer(NSmartContext context) {
        super(context);

        modelManager = (NDataModelManager) NDataModelManager.getInstance(context.getKylinConfig());
    }

    @Override
    void propose() {
        List<NSmartContext.NModelContext> modelContexts = context.getModelContexts();
        if (modelContexts == null || modelContexts.isEmpty())
            return;

        for (NSmartContext.NModelContext modelContext : modelContexts) {
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = compareWithFactTable(modelContext.getModelTree());
            if (model != null) {
                // found matched, then use it
                modelContext.setOrigModel(model);
                modelContext.setTargetModel(NDataModel.getCopyOf(model));
            }
        }
    }

    private NDataModel compareWithFactTable(ModelTree modelTree) {
        for (DataModelDesc model : modelManager.getModels(context.getProject())) {
            if (model.getRootFactTable().getTableIdentity().equals(modelTree.getRootFactTable().getIdentity()))
                return (NDataModel) model;
        }

        return null;
    }
}
