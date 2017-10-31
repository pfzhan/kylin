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

package io.kyligence.kap.metadata.model;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.model.ModelTree;
import io.kyligence.kap.smart.util.CubeUtils;

/**
 * Define Dimensions and Measures from SQLs
 */
public class NQueryScopeProposer extends NAbstractModelProposer {
    private static final Logger logger = LoggerFactory.getLogger(NQueryScopeProposer.class);

    public NQueryScopeProposer(NSmartContext.NModelContext modelCtx) {
        super(modelCtx);
    }

    private boolean checkFunctionDesc(FunctionDesc functionDesc) {
        List<TblColRef> colRefs = functionDesc.getParameter().getColRefs();
        if (colRefs == null || colRefs.isEmpty())
            return true;

        boolean isMaxMin = functionDesc.isMax() || functionDesc.isMin();
        for (TblColRef colRef : colRefs) {
            if (!colRef.isQualified()) {
                return false;
            }

            if (isMaxMin && colRef.getType().isStringFamily()) {
                return false;
            }
        }

        return true;
    }

    private ParameterDesc copyParameterDesc(ParameterDesc param) {
        ParameterDesc newParam = new ParameterDesc();
        newParam.setType(param.getType());
        if (param.isColumnType()) {
            newParam.setValue(param.getColRef().getIdentity());
        } else {
            newParam.setValue(param.getValue());
        }

        if (param.getNextParameter() != null)
            newParam.setNextParameter(copyParameterDesc(param.getNextParameter()));
        return newParam;
    }

    private FunctionDesc copyFunctionDesc(NDataModel model, FunctionDesc orig) {
        TblColRef paramColRef = orig.getParameter().getColRef();
        ParameterDesc newParam = copyParameterDesc(orig.getParameter());
        return CubeUtils.newFunctionDesc(model, orig.getExpression(), newParam,
                paramColRef == null ? null : paramColRef.getDatatype());
    }

    @Override
    protected void doPropose(NDataModel nDataModel) {
        ModelTree modelTree = modelContext.getModelTree();

        // column_identity <====> NamedColumn
        Map<String, NDataModel.NamedColumn> columnsCandidate = Maps.newHashMap();
        Map<FunctionDesc, NDataModel.Measure> measureCandidate = Maps.newHashMap();

        // Load from old model
        int maxColId = -1, maxMeasureId = NDataModel.MEASURE_ID_BASE - 1;
        for (NDataModel.NamedColumn namedColumn : nDataModel.getAllNamedColumns()) {
            columnsCandidate.put(namedColumn.aliasDotColumn, namedColumn);
            maxColId = Math.max(maxColId, namedColumn.id);
        }
        for (NDataModel.Measure measure : nDataModel.getAllMeasures()) {
            measureCandidate.put(measure.getFunction(), measure);
            maxMeasureId = Math.max(maxMeasureId, measure.id);
        }

        // Load from context
        for (OLAPContext ctx : modelTree.getOlapContexts()) { // note: all olap_context are fixed
            // fix models to update alias
            Map<String, String> matchingAlias = RealizationChooser.matches(nDataModel, ctx);
            ctx.fixModel(nDataModel, matchingAlias);

            // collect names columns
            Set<TblColRef> allColumns = Sets.newHashSet(ctx.allColumns);
            if (allColumns == null || allColumns.size() == 0) {
                allColumns = new HashSet<>();
                for (OLAPTableScan tableScan : ctx.allTableScans) {
                    allColumns.addAll(tableScan.getTableRef().getColumns());
                }
            }

            for (TblColRef tblColRef : allColumns) {
                if (columnsCandidate.containsKey(tblColRef.getIdentity()))
                    continue;

                int newId = maxColId + 1;
                NDataModel.NamedColumn col = new NDataModel.NamedColumn();
                col.name = tblColRef.getIdentity();
                col.aliasDotColumn = tblColRef.getIdentity();
                col.id = newId;

                columnsCandidate.put(tblColRef.getIdentity(), col);
                maxColId = newId;
            }

            // collect measures
            List<FunctionDesc> aggregations = Lists.newLinkedList(ctx.aggregations);
            for (FunctionDesc agg : aggregations) {
                if (measureCandidate.containsKey(agg))
                    continue;

                if (checkFunctionDesc(agg)) {
                    FunctionDesc newFunc = copyFunctionDesc(nDataModel, agg);
                    NDataModel.Measure measure = new NDataModel.Measure();
                    measure.id = maxMeasureId + 1;
                    measure.setName(UUID.randomUUID().toString());
                    measure.setFunction(newFunc);

                    measureCandidate.put(newFunc, measure);
                    maxMeasureId = Math.max(measure.id, maxMeasureId);
                }
            }
            ctx.unfixModel();
        }

        FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(nDataModel);
        if (!measureCandidate.containsKey(countStar)) {
            NDataModel.Measure measure = new NDataModel.Measure();
            measure.id = maxMeasureId + 1;

            measureCandidate.put(countStar, measure);
            maxMeasureId = Math.max(measure.id, maxMeasureId);
        }

        nDataModel.setAllNamedColumns(Lists.newArrayList(columnsCandidate.values()));
        nDataModel.setAllMeasures(Lists.newArrayList(measureCandidate.values()));
    }
}
