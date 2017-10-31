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

package io.kyligence.kap.cube.model;

import org.apache.commons.lang.StringUtils;

import io.kyligence.kap.smart.NSmartContext;

public class NCubeMaster {
    private final NSmartContext.NModelContext context;
    private final NProposerProvider proposerProvider;

    public NCubeMaster(NSmartContext.NModelContext context) {
        this.context = context;
        this.proposerProvider = NProposerProvider.create(this.context);
    }

    public NSmartContext.NModelContext getContext() {
        return this.context;
    }

    public NCubePlan proposeInitialCube() {
        NCubePlan cubePlan = new NCubePlan();
        cubePlan.updateRandomUuid();
        cubePlan.setName(cubePlan.getUuid());
        cubePlan.setModelName(context.getTargetModel().getName());
        cubePlan.setDescription(StringUtils.EMPTY);
        return cubePlan;
    }

    public NCubePlan proposeCuboids(final NCubePlan cubePlan) {
        return proposerProvider.getCuboidProposer().propose(cubePlan);
    }

    public NCubePlan proposeDimensions(final NCubePlan cubePlan) {
        return proposerProvider.getDimensionProposer().propose(cubePlan);
    }
}
