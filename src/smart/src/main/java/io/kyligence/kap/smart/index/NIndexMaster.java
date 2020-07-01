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

package io.kyligence.kap.smart.index;

import org.apache.commons.lang.StringUtils;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.smart.AbstractContext;

public class NIndexMaster {

    private final AbstractContext.NModelContext context;
    private final NProposerProvider proposerProvider;

    public NIndexMaster(AbstractContext.NModelContext context) {
        this.context = context;
        this.proposerProvider = NProposerProvider.create(this.context);
    }

    public AbstractContext.NModelContext getContext() {
        return this.context;
    }

    public IndexPlan proposeInitialIndexPlan() {
        IndexPlan indexPlan = new IndexPlan();
        indexPlan.setUuid(context.getTargetModel().getUuid());
        indexPlan.setDescription(StringUtils.EMPTY);
        return indexPlan;
    }

    public IndexPlan proposeCuboids(final IndexPlan indexPlan) {
        return proposerProvider.getCuboidProposer().execute(indexPlan);
    }

    public IndexPlan reduceCuboids(final IndexPlan indexPlan) {
        return proposerProvider.getCuboidReducer().execute(indexPlan);
    }
}