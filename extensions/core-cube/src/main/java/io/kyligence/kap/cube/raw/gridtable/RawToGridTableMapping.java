/**
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
package io.kyligence.kap.cube.raw.gridtable;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;

public class RawToGridTableMapping {

    private List<DataType> gtDataTypes = Lists.newArrayList();
    private List<TblColRef> orderedColumns = Lists.newArrayList();
    private List<TblColRef> otherColumns = Lists.newArrayList();
    private List<TblColRef> gtOrderColumns = Lists.newArrayList();
    private ImmutableBitSet gtPrimaryKey;

    public RawToGridTableMapping(RawTableInstance rawTableInstance) {
        int gtColIdx = 0;
        TblColRef orderedCol = rawTableInstance.getRawTableDesc().getOrderedColumn();
        orderedColumns.add(orderedCol);
        gtOrderColumns.add(orderedCol);

        BitSet pk = new BitSet();
        for (TblColRef o : orderedColumns) {
            gtDataTypes.add(o.getType());
            pk.set(gtColIdx);
            gtColIdx++;
        }
        gtPrimaryKey = new ImmutableBitSet(pk);

        for (TblColRef columnRef : rawTableInstance.getRawTableDesc().getColumns()) {
            if (!orderedColumns.contains(columnRef)) {
                otherColumns.add(columnRef);
                gtOrderColumns.add(columnRef);
                gtDataTypes.add(columnRef.getType());
            }
        }
    }

    public DataType[] getDataTypes() {
        return gtDataTypes.toArray(new DataType[gtDataTypes.size()]);
    }

    public ImmutableBitSet getPrimaryKey() {
        return gtPrimaryKey;
    }

    public List<TblColRef> getGtOrderColumns() {
        return gtOrderColumns;
    }

    public int getIndexOf(TblColRef col) {
        return getGtOrderColumns().indexOf(col);
    }

    public int getIndexOf(FunctionDesc metrics) {
        throw new UnsupportedOperationException();
    }

    public ImmutableBitSet makeGridTableColumns(Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = getIndexOf(dim);
            if (idx >= 0)
                result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    public ImmutableBitSet makeGridTableColumns(Collection<FunctionDesc> metrics) {
        if (metrics.size() > 0) {
            throw new UnsupportedOperationException("Aggr is not supported yet" + metrics);
        }
        return new ImmutableBitSet(new BitSet());
        //        BitSet result = new BitSet();
        //        for (FunctionDesc metric : metrics) {
        //            if (metric.getParameter().getColRefs().size() > 1) {
        //                throw new IllegalStateException("Currently Raw does not support Functions with more than 1 column ref");
        //            }
        //            
        //            for (TblColRef col : metric.getParameter().getColRefs()) {
        //                int idx = getIndexOf(col);
        //                if (idx < 0)
        //                    throw new IllegalStateException(metric + " not found in " + this);
        //                result.set(idx);
        //            }
        //        }
        //        return new ImmutableBitSet(result);
    }

    public String[] makeAggrFuncs(Collection<FunctionDesc> metrics) {
        if (metrics.size() > 0) {
            throw new UnsupportedOperationException("Aggr is not supported yet" + metrics);
        }
        return new String[0];
    }

}
