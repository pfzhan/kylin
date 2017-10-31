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

package io.kyligence.kap.storage.gtrecord;

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

    public RawTableTupleConverter(RawTableInstance rawTableInstance, Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo) {
        this.tupleInfo = returnTupleInfo;

        RawToGridTableMapping mapping = rawTableInstance.getRawTableDesc().getRawToGridTableMapping();

        nSelectedDims = selectedDimensions.size();

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
            if (dimIndex < 0) {
                throw new IllegalStateException("Col '" + dim + "' is not in the mapping.");
            }
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
