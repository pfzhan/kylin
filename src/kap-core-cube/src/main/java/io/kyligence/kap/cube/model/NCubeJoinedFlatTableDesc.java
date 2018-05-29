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

package io.kyligence.kap.cube.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
public class NCubeJoinedFlatTableDesc implements IJoinedFlatTableDesc, Serializable {

    protected final String tableName;
    protected final NCubePlan cubePlan;
    protected final SegmentRange segmentRange;

    private Map<TblColRef, Integer> columnIndexMap = Maps.newHashMap();
    private List<TblColRef> columns = Lists.newLinkedList();
    private List<Integer> indexes = Lists.newArrayList();

    public NCubeJoinedFlatTableDesc(NCubePlan cubePlan) {
        this(cubePlan, null);
    }

    public NCubeJoinedFlatTableDesc(NDataSegment segment) {
        this(segment.getCubePlan(), segment.getSegRange());
    }

    public NCubeJoinedFlatTableDesc(NCubePlan cubePlan, @Nullable SegmentRange segmentRange) {
        this.cubePlan = cubePlan;
        this.segmentRange = segmentRange;
        this.tableName = makeTableName();

        initParseCubePlan();
    }

    protected String makeTableName() {
        if (segmentRange == null) {
            return "kylin_intermediate_" + cubePlan.getName().toLowerCase();
        } else {
            return "kylin_intermediate_" + cubePlan.getName().toLowerCase() + "_" + segmentRange.toString();
        }
    }

    protected final void initAddColumn(TblColRef col) {
        if (columnIndexMap.containsKey(col))
            return;

        columnIndexMap.put(col, columnIndexMap.size());
        columns.add(col);
    }

    // check what columns from hive tables are required, and index them
    private void initParseCubePlan() {
        for (Map.Entry<Integer, TblColRef> dimEntry : cubePlan.getEffectiveDimCols().entrySet()) {
            initAddColumn(dimEntry.getValue());
        }

        for (Map.Entry<Integer, NDataModel.Measure> measureEntry : cubePlan.getEffectiveMeasures().entrySet()) {
            FunctionDesc func = measureEntry.getValue().getFunction();
            List<TblColRef> colRefs = func.getParameter().getColRefs();
            if (colRefs != null) {
                for (TblColRef colRef : colRefs) {
                    initAddColumn(colRef);
                }
            }
        }

        // TODO: add dictionary columns
    }

    public List<Integer> getIndexes() {
        for (TblColRef tblColRef : columns) {
            int id = cubePlan.getModel().getColumnIdByColumnName(tblColRef.getIdentity());
            if (-1 == id)
                throw new IllegalArgumentException(
                        "Column: " + tblColRef.getIdentity() + " is not in model: " + cubePlan.getModel().getName());
            indexes.add(id);
        }
        return indexes;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columns;
    }

    @Override
    public NDataModel getDataModel() {
        return cubePlan.getModel();
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            return -1;

        return index;
    }

    @Override
    public SegmentRange getSegRange() {
        return segmentRange;
    }

    @Override
    public TblColRef getDistributedBy() {
        return null;
    }

    @Override
    public ISegment getSegment() {
        return null;
    }

    @Override
    public TblColRef getClusterBy() {
        return null;
    }

}
