/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.gtrecord;

import java.util.Set;

import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.gridtable.RawToGridTableMapping;

public class RawTableTupleConverter {

    private TupleInfo tupleInfo;

    private final int[] gtColIdx;
    private final int[] tupleIdx;
    private final Object[] gtValues;
    private final MeasureType<?>[] measureTypes;

    private final int nSelectedDims;
    private final int nSelectedMetrics;

    public RawTableTupleConverter(RawTableInstance rawTableInstance, Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo) {
        this.tupleInfo = returnTupleInfo;

        RawToGridTableMapping mapping = rawTableInstance.getRawToGridTableMapping();

        nSelectedDims = selectedDimensions.size();
        nSelectedMetrics = selectedMetrics.size();

        gtColIdx = new int[selectedDimensions.size() + selectedMetrics.size()];
        tupleIdx = new int[selectedDimensions.size() + selectedMetrics.size()];
        gtValues = new Object[selectedDimensions.size() + selectedMetrics.size()];

        // measure types don't have this many, but aligned length make programming easier
        measureTypes = new MeasureType[selectedDimensions.size() + selectedMetrics.size()];

        ////////////

        int i = 0;

        // pre-calculate dimension index mapping to tuple
        for (TblColRef dim : selectedDimensions) {
            int dimIndex = mapping.getIndexOf(dim);
            gtColIdx[i] = dimIndex;
            tupleIdx[i] = tupleInfo.hasColumn(dim) ? tupleInfo.getColumnIndex(dim) : -1;

            i++;
        }

        for (FunctionDesc metric : selectedMetrics) {
            int metricIndex = mapping.getIndexOf(metric);
            gtColIdx[i] = metricIndex;

            if (metric.needRewrite()) {
                String rewriteFieldName = metric.getRewriteFieldName();
                tupleIdx[i] = tupleInfo.hasField(rewriteFieldName) ? tupleInfo.getFieldIndex(rewriteFieldName) : -1;
            } else {
                // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
                TblColRef col = metric.getParameter().getColRefs().get(0);
                tupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            }

            MeasureType<?> measureType = metric.getMeasureType();
            measureTypes[i] = measureType;

            i++;
        }
    }

    private static String toString(Object o) {
        return o == null ? null : o.toString();
    }

    public void translateResult(GTRecord record, Tuple tuple) {

        record.getValues(gtColIdx, gtValues);

        // dimensions
        for (int i = 0; i < nSelectedDims; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0) {
                tuple.setDimensionValue(ti, toString(gtValues[i]));
            }
        }

        // measures
        for (int i = nSelectedDims; i < gtColIdx.length; i++) {
            int ti = tupleIdx[i];
            if (ti >= 0 && measureTypes[i] != null) {
                measureTypes[i].fillTupleSimply(tuple, ti, gtValues[i]);
            }
        }
    }

}
