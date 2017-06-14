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

package io.kyligence.kap.modeling.smart.proposer;

import org.apache.kylin.cube.model.CubeDesc;

import io.kyligence.kap.modeling.smart.ModelingContext;
import io.kyligence.kap.modeling.smart.common.ModelingConfig;
import io.kyligence.kap.query.mockup.Utils;

public abstract class AbstractProposer {
    final ModelingContext context;
    final ModelingConfig modelingConfig;

    AbstractProposer(ModelingContext context) {
        this.context = context;
        this.modelingConfig = context.getModelingConfig();
    }

    public CubeDesc propose(CubeDesc sourceCubeDesc) {
        CubeDesc workCubeDesc = CubeDesc.getCopyOf(sourceCubeDesc);
        try {
            workCubeDesc.init(context.getKylinConfig());
        } catch (IllegalStateException e) {
            // if cube not tuned, then enlarge combination in override props to bypass init().
            Utils.setLargeCuboidCombinationConf(workCubeDesc.getOverrideKylinProps());
            workCubeDesc.init(context.getKylinConfig());
        }

        doPropose(workCubeDesc);
        return workCubeDesc;
    }

    abstract void doPropose(CubeDesc workCubeDesc);
}
