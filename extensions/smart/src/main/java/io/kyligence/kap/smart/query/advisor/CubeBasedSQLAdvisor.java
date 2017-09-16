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

package io.kyligence.kap.smart.query.advisor;

import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.RealizationCheck;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.SQLResult;

public class CubeBasedSQLAdvisor extends AbstractSQLAdvisor {

    private CubeDesc cubeDesc;
    private ISQLAdviceProposer adviceProposer;

    public CubeBasedSQLAdvisor(CubeDesc cubeDesc) {
        this.cubeDesc = cubeDesc;
        this.adviceProposer = new CubeBasedSQLAdviceProposer(cubeDesc);
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

        boolean success = true;
        boolean currentCubeSuccess = false;
        List<SQLAdvice> sqlAdvices = Lists.newArrayList();

        if (olapContexts != null) {
            for (OLAPContext ctx : olapContexts) {
                if (ctx.firstTableScan == null || ctx.realizationCheck == null) {
                    continue;
                }
                RealizationCheck checkResult = ctx.realizationCheck;
                List<SQLAdvice> currentContextAdvisors = adviceForOLAPContext(ctx);

                if (!checkResult.getCubeCapabilities().values().contains(true)) {
                    success = false;
                }

                if (CollectionUtils.isEmpty(currentContextAdvisors)) {
                    currentCubeSuccess = true;
                }
                sqlAdvices.addAll(currentContextAdvisors);
            }
        }
        if (success && currentCubeSuccess) {
            return Lists.newArrayList();
        } else if (currentCubeSuccess) {
            List<SQLAdvice> result = Lists.newArrayList();
            advice = adviceProposer.propose(
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_OTHER_CUBE_INCAPABLE),
                    null);
            if (advice != null) {
                result.add(advice);
            }
            return result;
        } else {
            return sqlAdvices;
        }
    }

    private List<SQLAdvice> adviceForOLAPContext(OLAPContext olapContext) {
        RealizationCheck checkResult = olapContext.realizationCheck;
        ModelBasedSQLAdviceProposer modelSap = new ModelBasedSQLAdviceProposer(cubeDesc.getModel());
        List<SQLAdvice> currentContextAdvisors = Lists.newArrayList();
        if (!olapContext.firstTableScan.getTableRef().getTableDesc().getIdentity()
                .equals(cubeDesc.getModel().getRootFactTable().getTableDesc().getIdentity())) {
            SQLAdvice advice = adviceProposer.propose(
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_FACT_TABLE_NOT_FOUND),
                    olapContext);
            if (advice != null)
                currentContextAdvisors.add(advice);
        }
        Boolean capable = checkResult.getCubeCapabilities().get(cubeDesc);
        if (capable != null && !capable) {
            RealizationCheck.IncapableReason incapableReason = checkResult.getCubeIncapableReasons().get(cubeDesc);
            if (incapableReason.getIncapableType() == RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN) {
                Collection<OLAPTableScan> notFoundTables = notFoundTables(cubeDesc.getModel().getAllTables(),
                        olapContext);
                if (CollectionUtils.isNotEmpty(notFoundTables)) {
                    RealizationCheck.IncapableReason tableNotFoundReason = RealizationCheck.IncapableReason
                            .notFoundTables(notFoundTables);
                    SQLAdvice advice = adviceProposer.propose(tableNotFoundReason, olapContext);
                    if (advice != null)
                        currentContextAdvisors.add(advice);
                }
                Collection<TblColRef> dimensions = findDimensions(incapableReason.getNotFoundColumns(), olapContext);
                Collection<TblColRef> measures = findMeasures(incapableReason.getNotFoundColumns(), olapContext);

                if (CollectionUtils.isNotEmpty(dimensions)) {
                    SQLAdvice advice = adviceProposer
                            .propose(RealizationCheck.IncapableReason.notContainAllDimension(dimensions), olapContext);
                    if (advice != null)
                        currentContextAdvisors.add(advice);
                }
                if (CollectionUtils.isNotEmpty(measures)) {
                    SQLAdvice advice = adviceProposer
                            .propose(RealizationCheck.IncapableReason.notContainAllMeasures(measures), olapContext);
                    if (advice != null)
                        currentContextAdvisors.add(advice);
                }
            } else {
                SQLAdvice advice = adviceProposer.propose(incapableReason, olapContext);
                if (advice != null)
                    currentContextAdvisors.add(advice);
            }
        }
        if (checkResult.getModelIncapableReasons().containsKey(cubeDesc.getModel())) {
            List<RealizationCheck.IncapableReason> incapableReasons = checkResult.getModelIncapableReasons()
                    .get(cubeDesc.getModel());
            for (RealizationCheck.IncapableReason incapableReason : incapableReasons) {
                currentContextAdvisors.add(modelSap.propose(incapableReason, olapContext));
            }
        }
        return currentContextAdvisors;
    }
}
