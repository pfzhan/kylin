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

package io.kyligence.kap.smart.model;

import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelReuseContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProposerProvider {

    private ProposerProvider(AbstractContext.ModelContext modelContext) {
        this.modelContext = modelContext;
    }

    private AbstractContext.ModelContext modelContext;

    public static ProposerProvider create(AbstractContext.ModelContext modelContext) {
        return new ProposerProvider(modelContext);
    }

    public AbstractModelProposer getJoinProposer() {
        return new JoinProposer(modelContext);
    }

    public AbstractModelProposer getScopeProposer() {
        return new QueryScopeProposer(modelContext);
    }

    public AbstractModelProposer getPartitionProposer() {
        return new PartitionProposer(modelContext);
    }

    public AbstractModelProposer getComputedColumnProposer() {
        return modelContext.getProposeContext() instanceof ModelReuseContext
                ? new ComputedColumnProposer.ComputedColumnProposerOfModelReuseContext(modelContext)
                : new ComputedColumnProposer(modelContext);
    }

    public AbstractModelProposer getShrinkComputedColumnProposer() {
        return new ShrinkComputedColumnProposer(modelContext);
    }
}
