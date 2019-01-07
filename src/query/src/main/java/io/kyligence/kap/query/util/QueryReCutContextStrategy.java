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

package io.kyligence.kap.query.util;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.collect.Lists;

public class QueryReCutContextStrategy implements ICutContextStrategy {
    private CutContextImplementor cutImplementor;

    public QueryReCutContextStrategy(CutContextImplementor recutContextImplementor) {
        this.cutImplementor = recutContextImplementor;
    }

    @Override
    public List<OLAPRel> cutOffContext(OLAPRel rootRel, RelNode parentOfRoot) {
        for (OLAPTableScan tableScan : rootRel.getContext().allTableScans) {
            tableScan.setColumnRowType(null);
        }
        // pre-order travel tree, recut context to smaller contexts
        OLAPContext originCtx = rootRel.getContext();
        cutImplementor.visitChild(rootRel);
        OLAPContext.clearThreadLocalContextById(originCtx.id);
        return Lists.newArrayList(rootRel);
    }

    @Override
    public boolean needCutOff(OLAPRel rootRel) {
        return rootRel.getContext() != null && rootRel.getContext().isHasJoin();
    }

    public CutContextImplementor getRecutContextImplementor() {
        return cutImplementor;
    }
}
