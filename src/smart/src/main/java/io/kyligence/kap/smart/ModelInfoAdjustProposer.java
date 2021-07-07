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

import org.apache.kylin.metadata.model.JoinTableDesc;

import io.kyligence.kap.metadata.model.NDataModel;

public class ModelInfoAdjustProposer extends AbstractProposer {

    public ModelInfoAdjustProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        for (AbstractContext.ModelContext modelCtx : proposeContext.getModelContexts()) {
            if (modelCtx.isTargetModelMissing()) {
                continue;
            }

            NDataModel model = modelCtx.getTargetModel();
            modelCtx.getProposeContext().changeModelMainType(model);
            setJoinTableType(model);
        }
    }

    private void setJoinTableType(NDataModel model) {
        for (JoinTableDesc joinTableDesc : model.getJoinTables()) {
            if (!joinTableDesc.getTableRef().equals(model.getRootFactTableRef())) {
                joinTableDesc.setKind(model.getProjectInstance().isSemiAutoMode() ? NDataModel.TableKind.LOOKUP
                        : NDataModel.TableKind.FACT);
            }
        }
    }

    @Override
    public String getIdentifierName() {
        return "ModelInfoAdjustProposer";
    }
}