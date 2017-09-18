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

package io.kyligence.kap.smart.model.proposer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.collect.Sets;

import io.kyligence.kap.smart.model.ModelContext;

/**
 * Define Dimensions and Measures from SQLs
 */
public class QueryScopeProposer extends AbstractModelProposer {

    public QueryScopeProposer(ModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void doPropose(DataModelDesc modelDesc) {

        Set<TblColRef> dimCandidate = new HashSet<>();
        Set<TblColRef> measCandidate = new HashSet<>();
        for (OLAPContext ctx : getModelContext().getAllOLAPContexts()) {
            List<FunctionDesc> aggregations = ctx.aggregations;
            Set<TblColRef> aggColumns = new HashSet<>();
            for (FunctionDesc agg : aggregations) {
                if (agg.getParameter() == null) {
                    continue;
                }

                List<TblColRef> candidates = agg.getParameter().getColRefs();
                boolean useAsMeasure = true;
                if (agg.isMax() || agg.isMin()) {
                    // Cube measure does not support max and min on string columns, and we need to
                    // add these columns in dimension as workaround.
                    for (TblColRef colRef : candidates) {
                        if (colRef.getType().isStringFamily()) {
                            useAsMeasure = false;
                            break;
                        }
                    }
                }

                if (useAsMeasure) {
                    aggColumns.addAll(candidates);
                }
            }
            Set<TblColRef> allColumns = ctx.allColumns;
            if (allColumns == null || allColumns.size() == 0) {
                allColumns = new HashSet<>();
                for (OLAPTableScan tableScan : ctx.allTableScans) {
                    allColumns.addAll(tableScan.getTableRef().getColumns());
                }
            }
            for (TblColRef tblColRef : allColumns) {
                if (dimCandidate.contains(tblColRef)) {
                    continue;
                } else if (ctx.filterColumns.contains(tblColRef)) {
                    dimCandidate.add(tblColRef);
                    continue;
                } else if (ctx.groupByColumns.contains(tblColRef)) {
                    dimCandidate.add(tblColRef);
                    continue;
                } else if (aggColumns.contains(tblColRef)) {
                    measCandidate.add(tblColRef);
                    continue;
                }
                dimCandidate.add(tblColRef);
            }
        }

        // Add dimensions
        List<ModelDimensionDesc> dimensions = new ArrayList<>();
        Map<String, Set<String>> dimensionsMap = new HashMap<>();
        for (TblColRef dimension : dimCandidate) {
            String tableName = getModelContext().getTableRefAlias(dimension.getTableRef());
            if (dimensionsMap.get(tableName) == null) {
                dimensionsMap.put(tableName, Sets.<String> newHashSet());
            }
            dimensionsMap.get(tableName).add(dimension.getName());
        }
        for (Entry<String, Set<String>> dimensionEntry : dimensionsMap.entrySet()) {
            ModelDimensionDesc dimension = new ModelDimensionDesc();
            dimension.setTable(dimensionEntry.getKey());
            dimension.setColumns(dimensionEntry.getValue().toArray(new String[0]));
            dimensions.add(dimension);
        }
        modelDesc.setDimensions(dimensions);

        // Add measures
        Set<String> metrics = Sets.newHashSet();
        for (TblColRef measure : measCandidate) {
            String tableName = getModelContext().getTableRefAlias(measure.getTableRef());
            String columnName = measure.getName();
            if (dimensionsMap.containsKey(tableName) && dimensionsMap.get(tableName).contains(columnName)) {
                continue; // skip this as already defined as dimension
            }
            metrics.add(tableName + "." + columnName);
        }
        modelDesc.setMetrics(metrics.toArray(new String[0]));
    }

}
