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
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NProposerProvider {

    private NProposerProvider(AbstractContext.NModelContext modelContext) {
        this.modelContext = modelContext;
    }

    private AbstractContext.NModelContext modelContext;

    public static NProposerProvider create(AbstractContext.NModelContext modelContext) {
        return new NProposerProvider(modelContext);
    }

    public NAbstractModelProposer getJoinProposer() {
        return new NJoinProposer(modelContext);
    }

    public NAbstractModelProposer getScopeProposer() {
        return new NQueryScopeProposer(modelContext);
    }

    public NAbstractModelProposer getPartitionProposer() {
        return new NPartitionProposer(modelContext);
    }

    public NAbstractModelProposer getComputedColumnProposer() {
        return modelContext.getProposeContext() instanceof ModelReuseContextOfSemiV2
                ? new NComputedColumnProposer.NComputedColumnProposerOfModelReuseContext(modelContext)
                : new NComputedColumnProposer(modelContext);
    }

    public NAbstractModelProposer getShrinkComputedColumnProposer() {
        return new NShrinkComputedColumnProposer(modelContext);
    }
}