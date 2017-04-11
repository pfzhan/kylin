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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.modeling.smart.proposer.AggrGroupProposer;
import io.kyligence.kap.modeling.smart.proposer.ConfigOverrideProposer;
import io.kyligence.kap.modeling.smart.proposer.DerivedDimensionProposer;
import io.kyligence.kap.modeling.smart.proposer.RowkeyProposer;

public class ModelingMaster {
    private static final Logger logger = LoggerFactory.getLogger(ModelingMaster.class);

    private ModelingContext context;

    ModelingMaster(ModelingContext context) {
        this.context = context;
    }

    public CubeDesc proposeInitialCube() {
        return context.getDomain().buildCubeDesc();
    }

    public CubeDesc proposeDerivedDimensions(final CubeDesc cubeDesc) {
        DerivedDimensionProposer cubeDescProposer = new DerivedDimensionProposer(context);
        return cubeDescProposer.propose(cubeDesc);
    }

    public CubeDesc proposeAggrGroup(final CubeDesc cubeDesc) {
        AggrGroupProposer cubeDescProposer = new AggrGroupProposer(context);
        return cubeDescProposer.propose(cubeDesc);
    }

    public CubeDesc proposeRowkey(final CubeDesc cubeDesc) {
        RowkeyProposer cubeDescProposer = new RowkeyProposer(context);
        return cubeDescProposer.propose(cubeDesc);
    }

    public CubeDesc proposeConfigOverride(final CubeDesc cubeDesc) {
        ConfigOverrideProposer cubeDescProposer = new ConfigOverrideProposer(context);
        return cubeDescProposer.propose(cubeDesc);
    }
}
