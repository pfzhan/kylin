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

package io.kyligence.kap.cube.model;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DataModelFlatTableDesc implements IJoinedFlatTableDesc {

    private String tableName;
    private final CubeDesc cubeDesc;
    private final DataModelDesc dataModelDesc;
    private final CubeSegment cubeSegment;

    private int columnCount;

    private List<TblColRef> columnList = Lists.newArrayList();
    private Map<TblColRef, Integer> columnIndexMap;

    public DataModelFlatTableDesc(CubeSegment cubeSegment) {
        this(cubeSegment.getCubeDesc(), cubeSegment);
    }

    private DataModelFlatTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment /* can be null */) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.columnIndexMap = Maps.newHashMap();
        this.dataModelDesc = cubeDesc.getModel();
        parseCubeDesc();
    }

    // check what columns from hive tables are required, and index them
    private void parseCubeDesc() {
        if (cubeSegment == null) {
            this.tableName = "kylin_intermediate_" + dataModelDesc.getName();
        } else {
            this.tableName = "kylin_intermediate_" + dataModelDesc.getName() + "_" + cubeSegment.getUuid().replaceAll("-", "_");
        }

        // add dimensions
        int columnIndex = 0;
        for (ModelDimensionDesc mdDesc : dataModelDesc.getDimensions()) {
            for (String col : mdDesc.getColumns()) {
                TblColRef tblColRef = dataModelDesc.findColumn(mdDesc.getTable(), col);
                columnIndexMap.put(tblColRef, columnIndex);
                columnList.add(tblColRef);
                columnIndex++;
            }
        }

        // add metrics
        for (String metric : dataModelDesc.getMetrics()) {
            TblColRef tblColRef = dataModelDesc.findColumn(metric);
            if (!columnIndexMap.containsKey(tblColRef)) {
                columnIndexMap.put(tblColRef, columnIndex);
                columnList.add(tblColRef);
                columnIndex++;
            }
        }

        int lookupLength = dataModelDesc.getLookups().length;
        for (int i = 0; i < lookupLength; i++) {
            JoinDesc join = dataModelDesc.getLookups()[i].getJoin();
            for (TblColRef primary : join.getPrimaryKeyColumns()) {
                if (!columnIndexMap.containsKey(primary)) {
                    columnIndexMap.put(primary, columnIndex);
                    columnList.add(primary);
                    columnIndex++;
                }
            }

            for (TblColRef foreign : join.getForeignKeyColumns()) {
                if (!columnIndexMap.containsKey(foreign)) {
                    columnIndexMap.put(foreign, columnIndex);
                    columnList.add(foreign);
                    columnIndex++;
                }
            }
        }
        columnCount = columnIndex;
    }

    // sanity check the input record (in bytes) matches what's expected
    public void sanityCheck(BytesSplitter bytesSplitter) {
        if (columnCount != bytesSplitter.getBufferSize()) {
            throw new IllegalArgumentException("Expect " + columnCount + " columns, but see " + bytesSplitter.getBufferSize() + " -- " + bytesSplitter);
        }

        // TODO: check data types here
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columnList;
    }

    @Override
    public DataModelDesc getDataModel() {
        return this.dataModelDesc;
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            throw new IllegalArgumentException("Column " + colRef.toString() + " wasn't found on flat table.");

        return index.intValue();
    }

    @Override
    public long getSourceOffsetStart() {
        return cubeSegment.getSourceOffsetStart();
    }

    @Override
    public long getSourceOffsetEnd() {
        return cubeSegment.getSourceOffsetEnd();
    }

    @Override
    public TblColRef getDistributedBy() {
        return cubeDesc.getDistributedByColumn();
    }

    @Override
    public ISegment getSegment() {
        return cubeSegment;
    }

}
