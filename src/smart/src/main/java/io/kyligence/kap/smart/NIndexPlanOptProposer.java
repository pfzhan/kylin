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

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.smart.index.NIndexMaster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NIndexPlanOptProposer extends NAbstractProposer {

    public NIndexPlanOptProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        if (proposeContext.getModelContexts() == null)
            return;

        for (AbstractContext.NModelContext modelCtx : proposeContext.getModelContexts()) {
            NIndexMaster indexMaster = new NIndexMaster(modelCtx);
            if (modelCtx.isTargetModelMissing() || modelCtx.isSnapshotSelected()) {
                continue;
            }

            try {
                IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
                if (indexPlan == null) {
                    indexPlan = indexMaster.proposeInitialIndexPlan();
                }

                indexPlan = indexMaster.proposeCuboids(indexPlan);
                modelCtx.setTargetIndexPlan(indexPlan);
            } catch (Exception e) {
                log.error("[UNLIKELY_THINGS_HAPPENED] Something wrong occurs in initialize target indexPlan.", e);
                modelCtx.setTargetIndexPlan(null);
                proposeContext.recordException(modelCtx, e);
            }
        }
    }

    @Override
    public String getIdentifierName() {
        return "IndexPlanOptProposer";
    }
}
