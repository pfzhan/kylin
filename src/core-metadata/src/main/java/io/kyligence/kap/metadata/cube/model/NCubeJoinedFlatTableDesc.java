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

package io.kyligence.kap.metadata.cube.model;

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
import lombok.val;

@SuppressWarnings("serial")
public class NCubeJoinedFlatTableDesc implements IJoinedFlatTableDesc, Serializable {

    protected final String tableName;
    protected final IndexPlan indexPlan;
    protected final SegmentRange segmentRange;
    protected final boolean needJoin;

    private Map<TblColRef, Integer> columnIndexMap = Maps.newHashMap();
    private List<TblColRef> columns = Lists.newLinkedList();
    private List<Integer> indices = Lists.newArrayList();

    public NCubeJoinedFlatTableDesc(IndexPlan indexPlan) {
        this(indexPlan, null, true);
    }

    public NCubeJoinedFlatTableDesc(NDataSegment segment) {
        this(segment.getIndexPlan(), segment.getSegRange(), true);
    }

    public NCubeJoinedFlatTableDesc(IndexPlan indexPlan, @Nullable SegmentRange segmentRange, Boolean needJoinLookup) {
        this.indexPlan = indexPlan;
        this.segmentRange = segmentRange;
        this.tableName = makeTableName();
        this.needJoin = needJoinLookup;

        initParseIndexPlan();

        initIndices();
    }

    protected String makeTableName() {
        if (segmentRange == null) {
            return "kylin_intermediate_" + indexPlan.getUuid().toLowerCase();
        } else {
            return "kylin_intermediate_" + indexPlan.getUuid().toLowerCase() + "_" + segmentRange.toString();
        }
    }

    protected final void initAddColumn(TblColRef col) {
        val model = getDataModel();
        val factTable = model.getRootFactTable();
        if (!needJoin && !factTable.getTableName().equalsIgnoreCase(col.getTableRef().getTableName())) {
            return;
        }

        if (columnIndexMap.containsKey(col))
            return;

        columnIndexMap.put(col, columnIndexMap.size());
        columns.add(col);
    }

    // check what columns from hive tables are required, and index them
    private void initParseIndexPlan() {
        for (Map.Entry<Integer, TblColRef> dimEntry : indexPlan.getEffectiveDimCols().entrySet()) {
            initAddColumn(dimEntry.getValue());
        }

        for (Map.Entry<Integer, NDataModel.Measure> measureEntry : indexPlan.getEffectiveMeasures().entrySet()) {
            FunctionDesc func = measureEntry.getValue().getFunction();
            List<TblColRef> colRefs = func.getColRefs();
            if (colRefs != null) {
                for (TblColRef colRef : colRefs) {
                    initAddColumn(colRef);
                }
            }
        }

        // TODO: add dictionary columns
    }

    public List<Integer> getIndices() {
        return indices;
    }

    public void initIndices() {
        for (TblColRef tblColRef : columns) {
            int id = indexPlan.getModel().getColumnIdByColumnName(tblColRef.getIdentity());
            if (-1 == id)
                throw new IllegalArgumentException(
                        "Column: " + tblColRef.getIdentity() + " is not in model: " + indexPlan.getModel().getUuid());
            indices.add(id);
        }
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
        return indexPlan.getModel();
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
