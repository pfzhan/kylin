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

import io.kyligence.kap.cube.model.IndexPlan;
import io.kyligence.kap.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class NIndexPlanSelectProposer extends NAbstractProposer {

    private final NProjectManager projectManager;

    public NIndexPlanSelectProposer(NSmartContext smartContext) {
        super(smartContext);

        projectManager = NProjectManager.getInstance(kylinConfig);
    }

    @Override
    void propose() {
        List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        if (modelContexts == null || modelContexts.isEmpty())
            return;

        for (NSmartContext.NModelContext modelContext : modelContexts) {
            IndexPlan indexPlan = findExisting(modelContext.getTargetModel());
            if (indexPlan != null) {
                modelContext.setOrigIndexPlan(indexPlan);
                modelContext.setTargetIndexPlan(indexPlan.copy());
            }
        }
    }

    private IndexPlan findExisting(NDataModel model) {
        if (model == null) {
            return null;
        }
        val indexManager = NIndexPlanManager.getInstance(kylinConfig, project);
        return indexManager.getIndexPlan(model.getUuid());
    }
}
