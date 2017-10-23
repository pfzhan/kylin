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
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationCheck;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.query.SQLResult;

public class ModelBasedSQLAdvisor extends AbstractSQLAdvisor {
    private final DataModelDesc dataModelDesc;
    private ISQLAdviceProposer adviceProposer;

    public ModelBasedSQLAdvisor(DataModelDesc dataModelDesc) {
        this.dataModelDesc = dataModelDesc;
        this.adviceProposer = new ModelBasedSQLAdviceProposer(dataModelDesc);
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

        List<SQLAdvice> SQLAdvices = Lists.newArrayList();
        boolean allContextSuccess = true;
        boolean currentModelSuccess = false;
        OLAPContext failContext = null;

        if (olapContexts != null) {
            for (OLAPContext ctx : olapContexts) {
                if (ctx.firstTableScan == null || ctx.realizationCheck == null) {
                    continue;
                }
                RealizationCheck checkResult = ctx.realizationCheck;
                List<SQLAdvice> currentContextAdvisors = adviceForOLAPContext(ctx);

                if (CollectionUtils.isEmpty(currentContextAdvisors)) {
                    currentModelSuccess = true;
                }
                if (MapUtils.isEmpty(checkResult.getCapableModels())) {
                    allContextSuccess = false;
                    if (currentModelSuccess) {
                        failContext = ctx;
                    }
                }
                SQLAdvices.addAll(currentContextAdvisors);
            }
        }

        if (allContextSuccess && currentModelSuccess) {
            return Lists.newArrayList();
        } else if (currentModelSuccess) {
            RealizationCheck.IncapableReason otherModelIncapable = RealizationCheck.IncapableReason
                    .create(RealizationCheck.IncapableType.MODEL_OTHER_MODEL_INCAPABLE);
            advice = adviceProposer.propose(otherModelIncapable, failContext);
            List<SQLAdvice> result = Lists.newArrayList();
            if (advice != null) {
                result.add(advice);
            }
            return result;
        } else {
            return SQLAdvices;
        }
    }

    private List<SQLAdvice> adviceForOLAPContext(OLAPContext olapContext) {
        RealizationCheck checkResult = olapContext.realizationCheck;

        List<SQLAdvice> currentContextAdvisors = Lists.newArrayList();
        if (checkResult.getModelIncapableReasons().containsKey(dataModelDesc)) {
            List<RealizationCheck.IncapableReason> incapableReasons = checkResult.getModelIncapableReasons()
                    .get(dataModelDesc);
            for (RealizationCheck.IncapableReason incapableReason : incapableReasons) {
                SQLAdvice advice = adviceProposer.propose(incapableReason, olapContext);
                if (advice != null)
                    currentContextAdvisors.add(advice);
            }
        }
        //validate cube
        if (MapUtils.isNotEmpty(checkResult.getCubeIncapableReasons())) {
            for (Map.Entry<CubeDesc, RealizationCheck.IncapableReason> entry : checkResult.getCubeIncapableReasons()
                    .entrySet()) {
                CubeDesc cubeDesc = entry.getKey();
                if (cubeDesc.getModel().equals(dataModelDesc)) {
                    RealizationCheck.IncapableReason incapableReason = entry.getValue();
                    //just check not all column found and unmatched dimensions
                    if (incapableReason
                            .getIncapableType() == RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN) {
                        SQLAdvice notFoundTableAdvice = adviceNotFoundTable(dataModelDesc.getAllTables(), olapContext);
                        if (notFoundTableAdvice != null)
                            currentContextAdvisors.add(notFoundTableAdvice);

                        Set<TblColRef> dimensions = Sets.newHashSet();
                        Set<FunctionDesc> measures = Sets.newHashSet();
                        splitMeasureDimension(incapableReason.getNotFoundColumns(), olapContext, dimensions, measures);
                        if (CollectionUtils.isNotEmpty(dimensions)) {
                            SQLAdvice advice = adviceProposer.propose(
                                    RealizationCheck.IncapableReason.notContainAllDimension(dimensions), olapContext);
                            if (advice != null)
                                currentContextAdvisors.add(advice);
                        }
                        if (CollectionUtils.isNotEmpty(measures)) {
                            SQLAdvice advice = adviceProposer.propose(
                                    RealizationCheck.IncapableReason.notContainAllMeasures(measures), olapContext);
                            if (advice != null)
                                currentContextAdvisors.add(advice);
                        }
                    } else if (incapableReason
                            .getIncapableType() == RealizationCheck.IncapableType.CUBE_UNMATCHED_DIMENSION) {
                        SQLAdvice advice = adviceProposer.propose(incapableReason, olapContext);
                        if (advice != null)
                            currentContextAdvisors.add(advice);
                    }
                }
            }
        }

        if (!olapContext.firstTableScan.getTableRef().getTableDesc().getIdentity()
                .equals(dataModelDesc.getRootFactTable().getTableDesc().getIdentity())) {
            SQLAdvice advice = adviceProposer.propose(
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_FACT_TABLE_NOT_FOUND),
                    olapContext);
            if (advice != null)
                currentContextAdvisors.add(advice);
        }
        return currentContextAdvisors;
    }
}
