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

package io.kyligence.kap.smart.advisor;

import java.util.Collection;
import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationCheck;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.SQLResult;

public class RawModelSQLAdvisor extends AbstractSQLAdvisor {

    private ISQLAdviceProposer adviceProposer;
    private TableDesc factTable;

    public RawModelSQLAdvisor(TableDesc fact) {
        this.adviceProposer = new RawModelSQLAdviceProposer();
        this.factTable = fact;
    }

    @Override
    protected ISQLAdviceProposer getAdviceProposer() {
        return adviceProposer;
    }

    @Override
    public List<SQLAdvice> provideAdvice(SQLResult sqlResult, Collection<OLAPContext> olapContexts) {
        SQLAdvice advice = adviseSyntaxError(sqlResult);
        if (advice != null) {
            return Lists.newArrayList(advice);
        }

        if (olapContexts == null) {
            return Lists.newArrayList();
        }

        List<SQLAdvice> SQLAdvices = Lists.newArrayList();
        boolean allContextSuccess = true;
        boolean oneContextSuccess = false;

        for (OLAPContext ctx : olapContexts) {
            if (ctx.firstTableScan == null) {
                continue;
            }

            SQLAdvice currentAdvice = adviceOfFactTableNotFound(ctx);
            if (currentAdvice != null) {
                allContextSuccess = false;
                SQLAdvices.add(currentAdvice);
                break;
            }
            oneContextSuccess = true;
        }

        if (allContextSuccess || oneContextSuccess) {
            return Lists.newArrayList();
        } else {
            return SQLAdvices;
        }
    }

    private SQLAdvice adviceOfFactTableNotFound(OLAPContext olapContext) {
        if (factTable == null) {
            return null;
        }
        if (!olapContext.firstTableScan.getTableRef().getTableDesc().getIdentity().equals(factTable.getIdentity())) {
            return adviceProposer.propose(RealizationCheck.IncapableReason
                    .create(RealizationCheck.IncapableType.FACT_TABLE_NOT_CONSISTENT_IN_MODEL_AND_QUERY), olapContext);
        }
        return null;
    }
}
