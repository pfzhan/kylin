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
