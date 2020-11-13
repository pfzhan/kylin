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
import java.util.Set;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelRenameProposer extends AbstractProposer {

    private static final String MODEL_ALIAS_PREFIX = "AUTO_MODEL_";

    public ModelRenameProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Set<String> usedNames = dataModelManager.listAllModelAlias();
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        for (AbstractContext.ModelContext modelCtx : modelContexts) {
            if (modelCtx.isTargetModelMissing()) {
                continue;
            }

            NDataModel targetModel = modelCtx.getTargetModel();
            String alias = modelCtx.getOriginModel() == null //
                    ? proposeModelAlias(targetModel, usedNames) //
                    : modelCtx.getOriginModel().getAlias();
            targetModel.setAlias(alias);
        }
    }

    private String proposeModelAlias(NDataModel model, Set<String> usedNames) {
        String rootTableAlias = model.getRootFactTable().getAlias();
        int suffix = 0;
        String targetName;
        do {
            if (suffix++ < 0) {
                throw new IllegalStateException("Potential infinite loop in getModelName().");
            }
            targetName = ModelRenameProposer.MODEL_ALIAS_PREFIX + rootTableAlias + "_" + suffix;
        } while (usedNames.contains(targetName));
        log.info("The alias of the model({}) was rename to {}.", model.getId(), targetName);
        usedNames.add(targetName);
        return targetName;
    }

    @Override
    public String getIdentifierName() {
        return "ModelRenameProposer";
    }
}
