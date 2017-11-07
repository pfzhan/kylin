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

package io.kyligence.kap.smart.cube.proposer;

import io.kyligence.kap.smart.cube.CubeContext;

public class ProposerProvider {

    ProposerProvider(CubeContext context) {
        this.context = context;
    }

    private CubeContext context;

    public static ProposerProvider create(CubeContext context) {
        return new ProposerProvider(context);
    }

    public AbstractCubeProposer getDimensionProposer() {
        return new DerivedDimensionProposer(context);
    }

    public AbstractCubeProposer getConfigOverrideProposer() {
        return new ConfigOverrideProposer(context);
    }

    public AbstractCubeProposer getAggrGroupProposer() {
        String strategy = context.getSmartConfig().getAggGroupStrategy();
        switch (strategy) {
        case "whitelist":
            return new QueryAggrGroupProposer(context);
        case "mixed":
            return new MixedAggrGroupProposer(context);
        case "auto":
            return autoSelectAggrProposer(context);
        default:
            return new ModelAggrGroupProposer(context);
        }
    }

    private static AbstractCubeProposer autoSelectAggrProposer(CubeContext context) {
        boolean hasModelStats = context.hasModelStats();
        boolean hasQueryStats = context.hasQueryStats();

        if (hasModelStats && hasQueryStats) {
            return new MixedAggrGroupProposer(context);
        } else if (hasQueryStats) {
            return new QueryAggrGroupProposer(context);
        } else {
            return new ModelAggrGroupProposer(context);
        }
    }

    public AbstractCubeProposer getRowkeyProposer() {
        return new RowkeyProposer(context);
    }

}
