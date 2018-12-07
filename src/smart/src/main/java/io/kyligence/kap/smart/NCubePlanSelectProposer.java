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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.metadata.realization.IRealization;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;

public class NCubePlanSelectProposer extends NAbstractProposer {

    private final NProjectManager projectManager;

    public NCubePlanSelectProposer(NSmartContext smartContext) {
        super(smartContext);

        projectManager = NProjectManager.getInstance(kylinConfig);
    }

    @Override
    void propose() {
        List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        if (modelContexts == null || modelContexts.isEmpty())
            return;

        for (NSmartContext.NModelContext modelContext : modelContexts) {
            NCubePlan cubePlan = findExisting(modelContext.getTargetModel());
            if (cubePlan != null) {
                modelContext.setOrigCubePlan(cubePlan);
                modelContext.setTargetCubePlan(cubePlan.copy());
            }
        }
    }

    private NCubePlan findExisting(NDataModel model) {
        if (model == null) {
            return null;
        }

        final Set<IRealization> iRealizations = projectManager.listAllRealizations(project);
        if (iRealizations.isEmpty()) {
            return null;
        }
        //TODO order by cost in the future
        // keep order by name of realization, so that can get consistent result under different circumstance
        TreeSet<IRealization> realizations = new TreeSet<>(Comparator.comparing(IRealization::getName));
        realizations.addAll(iRealizations);

        for (IRealization realization : realizations) {
            if (realization instanceof NDataflow) {
                NCubePlan cubePlan = ((NDataflow) realization).getCubePlan();
                if (cubePlan == null) {
                    continue;
                }
                if (cubePlan.getModelName().equals(model.getName()))
                    return cubePlan;
            }
        }

        return null;
    }
}
