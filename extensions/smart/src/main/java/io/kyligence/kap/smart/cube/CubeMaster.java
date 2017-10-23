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

package io.kyligence.kap.smart.cube;

import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;

import io.kyligence.kap.smart.cube.proposer.ProposerProvider;

public class CubeMaster {
    private final CubeContext context;
    private final ProposerProvider proposerProvider;

    public CubeMaster(CubeContext context) {
        this.context = context;
        this.proposerProvider = ProposerProvider.create(this.context);
    }

    public CubeContext getContext() {
        return this.context;
    }

    private static String createCubeName(CubeContext context) {
        String tmpName = context.getCubeName();
        if (tmpName == null) {
            tmpName = context.getModelDesc().getName();
        }

        CubeDescManager cubeDescManager = CubeDescManager.getInstance(context.getKylinConfig());
        int dupId = 1;
        String fixPart = tmpName;
        while (cubeDescManager.getCubeDesc(tmpName) != null) {
            tmpName = String.format("%s_%d", fixPart, dupId++);
        }
        return tmpName;
    }

    public CubeDesc proposeInitialCube() {
        String cubeName = createCubeName(context);
        return context.getDomain().buildCubeDesc(cubeName);
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

    public CubeDesc proposeAll() {
        CubeDesc cubeDesc = proposeInitialCube();
        CubeDesc cubeDescWithDerived = proposeDerivedDimensions(cubeDesc);
        CubeDesc cubeDescWithAggrGroup = proposeAggrGroup(cubeDescWithDerived);
        CubeDesc cubeDescWithRowkey = proposeRowkey(cubeDescWithAggrGroup);
        CubeDesc cubeDescWithConfig = proposeConfigOverride(cubeDescWithRowkey);
        return cubeDescWithConfig;
    }
}
