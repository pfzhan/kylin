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

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.smart.cube.NCubeMaster;

public class NCubePlanShrinkProposer extends NAbstractProposer {

    public NCubePlanShrinkProposer(NSmartContext modelCtx) {
        super(modelCtx);
    }

    @Override
    void propose() {
        if (context.getModelContexts() == null)
            return;

        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.getOrigModel() == null) {
                continue;
            }
            if (modelCtx.getOrigCubePlan() == null) {
                continue;
            }
            if (modelCtx.getTargetCubePlan() == null) {
                continue;
            }
            
            NCubeMaster cubeMaster = new NCubeMaster(modelCtx);
            NCubePlan cubePlan = modelCtx.getTargetCubePlan();
            cubePlan = cubeMaster.reduceCuboids(cubePlan);
            modelCtx.setTargetCubePlan(cubePlan);
        }
    }
}
