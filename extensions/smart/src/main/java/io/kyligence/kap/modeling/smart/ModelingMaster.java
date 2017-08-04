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

package io.kyligence.kap.modeling.smart;

import org.apache.kylin.cube.model.CubeDesc;

import io.kyligence.kap.modeling.smart.proposer.ProposerProvider;
import io.kyligence.kap.modeling.smart.proposer.ProposerProviderFactory;

public class ModelingMaster {
    private final ModelingContext context;
    private final ProposerProvider proposerProvider;

    ModelingMaster(ModelingContext context) {
        this.context = context;
        this.proposerProvider = ProposerProviderFactory.createProvider(this.context);
    }

    public ModelingContext getContext() {
        return this.context;
    }

    public CubeDesc proposeInitialCube() {
        return context.getDomain().buildCubeDesc();
    }

    public CubeDesc proposeDerivedDimensions(final CubeDesc cubeDesc) {
        return proposerProvider.getDimensionProposer().propose(cubeDesc);
    }

    public CubeDesc proposeAggrGroup(final CubeDesc cubeDesc) {
        return proposerProvider.getAggrGroupProposer().propose(cubeDesc);
    }

    public CubeDesc proposeRowkey(final CubeDesc cubeDesc) {
        return proposerProvider.getRowkeyProposer().propose(cubeDesc);
    }

    public CubeDesc proposeConfigOverride(final CubeDesc cubeDesc) {
        return proposerProvider.getConfigOverrideProposer().propose(cubeDesc);
    }
}
