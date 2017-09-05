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
package io.kyligence.kap.cube.raw.gridtable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableColumnDesc;
import io.kyligence.kap.cube.raw.RawTableColumnFamilyDesc;
import io.kyligence.kap.cube.raw.RawTableDesc;

public class RawToGridTableMapping {

    private List<DataType> gtDataTypes = Lists.newArrayList();
    private List<DataType> originDataTypes = Lists.newArrayList();
    private List<TblColRef> gtOrderColumns = Lists.newArrayList();
    private List<TblColRef> originColumns = Lists.newArrayList();
    private ImmutableBitSet gtPrimaryKey;
    private ImmutableBitSet sortbyKey;
    private ImmutableBitSet nonSortbyKey;
    private ImmutableBitSet shardbyKey;
    
    private List<ImmutableBitSet> gtColBlocks = Lists.newArrayList();

    public RawToGridTableMapping(RawTableDesc rawTableDesc) {

        List<RawTableColumnDesc> originRawTableColumns = rawTableDesc.getOriginColumns();
        for (RawTableColumnDesc rawTableColumn : originRawTableColumns) {
            originColumns.add(rawTableColumn.getColumn());
            originDataTypes.add(rawTableColumn.getColumn().getType());
        }
        
        int gtColIdx = 0;
        
        List<TblColRef> allColumns = rawTableDesc.getColumnsInOrder();
        BitSet sortby = new BitSet();
        BitSet shardby = new BitSet();
        BitSet nonSortby = new BitSet();
        BitSet gtPrimary = new BitSet();

        for (TblColRef col : allColumns) {
            gtOrderColumns.add(col);
            gtDataTypes.add(col.getType());

            if (rawTableDesc.isSortby(col)) {
                sortby.set(gtColIdx);
            } else {
                nonSortby.set(gtColIdx);
            }

            if (rawTableDesc.isShardby(col)) {
                shardby.set(gtColIdx);
            }

            gtColIdx++;
        }
        sortbyKey = new ImmutableBitSet(sortby);
        nonSortbyKey = new ImmutableBitSet(nonSortby);
        shardbyKey = new ImmutableBitSet(shardby);
        
        // initiate column blocks        
        ArrayList<BitSet> columnBlocks = Lists.newArrayList();
        int cfNum = rawTableDesc.getRawTableMapping().getColumnFamily().length;
        for (int i = 0; i < cfNum; i++) {
            columnBlocks.add(new BitSet());
        }
        
        RawTableColumnFamilyDesc[] cfDescs = rawTableDesc.getRawTableMapping().getColumnFamily();
        for (int i = 0; i < cfDescs.length; i++) {
            int[] columnIndex = cfDescs[i].getColumnIndex();
            for (int idx : columnIndex) {
                columnBlocks.get(i).set(idx);
            }
        }

        // Set all columns in first column family as primary key in order to adapt to GTInfo: 
        RawTableColumnFamilyDesc firstCf = rawTableDesc.getRawTableMapping().getColumnFamily()[0];
        for (int idx : firstCf.getColumnIndex()) {
            gtPrimary.set(idx);
        }
        gtPrimaryKey = new ImmutableBitSet(gtPrimary);
        
        for (BitSet set : columnBlocks) {
            gtColBlocks.add(new ImmutableBitSet(set));
        }
    }

    public DataType[] getDataTypes() {
        return gtDataTypes.toArray(new DataType[gtDataTypes.size()]);
    }
    
    public DataType[] getOriginDataTypes() {
        return originDataTypes.toArray(new DataType[originDataTypes.size()]);
    }

    public ImmutableBitSet getPrimaryKey() {
        return gtPrimaryKey;
    }

    public ImmutableBitSet getSortbyColumnSet() {
        return sortbyKey;
    }

    public ImmutableBitSet getShardbyKey() {
        return shardbyKey;
    }

    public ImmutableBitSet getNonSortbyColumnSet() {
        return nonSortbyKey;
    }

    public List<TblColRef> getGtOrderColumns() {
        return gtOrderColumns;
    }
    
    public List<TblColRef> getOriginColumns() {
        return originColumns;
    }

    public int getIndexOf(TblColRef col) {
        return getGtOrderColumns().indexOf(col);
    }
    
    public int getOriginIndexOf(TblColRef col) {
        return getOriginColumns().indexOf(col);
    }
    
    public int getIndexOf(FunctionDesc metrics) {
        throw new UnsupportedOperationException();
    }
    
    public ImmutableBitSet[] getColumnBlocks() {
        return gtColBlocks.toArray(new ImmutableBitSet[gtColBlocks.size()]);
    }

    public ImmutableBitSet makeGridTableColumns(Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = getOriginIndexOf(dim);
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
    }

    public String[] makeAggrFuncs(Collection<FunctionDesc> metrics) {
        if (metrics.size() > 0) {
            throw new UnsupportedOperationException("Aggr is not supported yet" + metrics);
        }
        return new String[0];
    }

}
