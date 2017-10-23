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

        Map<String, Set<String>> dimCandidate = new HashMap<>();
        Map<String, Set<String>> measCandidate = new HashMap<>();

        // Load from old model
        for (ModelDimensionDesc dimensionDesc : modelDesc.getDimensions()) {
            String tableName = dimensionDesc.getTable();
            for (String columnName : dimensionDesc.getColumns()) {
                addCandidate(dimCandidate, tableName, columnName);
            }
        }
        for (String measure : modelDesc.getMetrics()) {
            int splitterIdx = measure.indexOf('.');
            if (splitterIdx < 1) {
                continue;
            }
            String table = measure.substring(0, splitterIdx);
            String column = measure.substring(splitterIdx + 1);
            addCandidate(measCandidate, table, column);
        }

        // Load from context
        for (OLAPContext ctx : getModelContext().getAllOLAPContexts()) {
            if (ctx == null || ctx.joins.size() != ctx.allTableScans.size() - 1) {
                continue;
            }

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
                if (ctx.filterColumns.contains(tblColRef) || ctx.groupByColumns.contains(tblColRef)) {
                    addCandidate(dimCandidate, tblColRef);
                    continue;
                } else if (aggColumns.contains(tblColRef)) {
                    addCandidate(measCandidate, tblColRef);
                    continue;
                }
                addCandidate(dimCandidate, tblColRef);
            }
        }

        // Add dimensions
        List<ModelDimensionDesc> dimensions = new ArrayList<>();
        for (Entry<String, Set<String>> dimensionEntry : dimCandidate.entrySet()) {
            ModelDimensionDesc dimension = new ModelDimensionDesc();
            dimension.setTable(dimensionEntry.getKey());
            dimension.setColumns(dimensionEntry.getValue().toArray(new String[0]));
            dimensions.add(dimension);
        }
        modelDesc.setDimensions(dimensions);

        // Add measures
        Set<String> metrics = Sets.newHashSet();
        for (Entry<String, Set<String>> measureEntry : measCandidate.entrySet()) {
            String tableName = measureEntry.getKey();
            for (String columnName : measureEntry.getValue()) {
                if (dimCandidate.containsKey(tableName) && dimCandidate.get(tableName).contains(columnName)) {
                    continue; // skip this as already defined as dimension
                }
                metrics.add(tableName + "." + columnName);
            }
        }
        modelDesc.setMetrics(metrics.toArray(new String[0]));
    }

    private void addCandidate(Map<String, Set<String>> tblColMap, TblColRef column) {
        String table = getModelContext().getModelTree().getTableRefAliasMap().get(column.getTableRef());
        addCandidate(tblColMap, table, column.getName());
    }

    private static void addCandidate(Map<String, Set<String>> tblColMap, String table, String column) {
        if (tblColMap.get(table) == null) {
            tblColMap.put(table, Sets.<String> newHashSet());
        }
        tblColMap.get(table).add(column);
    }
}
