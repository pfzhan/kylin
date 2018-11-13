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

package io.kyligence.kap.metadata.model;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class NDataModelFlatTableDesc implements IJoinedFlatTableDesc {

    private static final Logger logger = LoggerFactory.getLogger(NDataModelFlatTableDesc.class);

    private NDataModel dataModelDesc;
    private String jobId;
    private SegmentRange segmentRange;
    private List<TblColRef> columnList = Lists.newArrayList();
    private Map<TblColRef, Integer> columnIndexMap = Maps.newHashMap();
    
    public NDataModelFlatTableDesc(NDataModel dataModelDesc) {
        this(dataModelDesc, false);
    }

    public NDataModelFlatTableDesc(NDataModel dataModelDesc, boolean ignoreComputedColumn) {
        this(dataModelDesc, null, null, ignoreComputedColumn);
    }

    public NDataModelFlatTableDesc(NDataModel dataModelDesc, SegmentRange segmentRange, String jobId, boolean ignoreComputedColumn) {
        this.dataModelDesc = dataModelDesc;
        this.jobId = jobId;
        this.segmentRange = segmentRange;
        init(ignoreComputedColumn);
    }

    private void init(boolean ignoreComputedColumn) {
        Preconditions.checkNotNull(dataModelDesc, "");
        for (TblColRef tblColRef : dataModelDesc.getEffectiveColsMap().values()) {
            Preconditions.checkNotNull(tblColRef, "");
            if (ignoreComputedColumn && tblColRef.getColumnDesc().isComputedColumn()) {
                continue;
            }
            addColumn(tblColRef);
        }
    }

    private void addColumn(TblColRef col) {
        if (columnIndexMap.containsKey(col))
            return;

        int columnIndex = columnIndexMap.size();
        columnIndexMap.put(col, columnIndex);
        columnList.add(col);
        Preconditions.checkState(columnIndexMap.size() == columnList.size());
    }

    @Override
    public String getTableName() {
        if (jobId == null)
            throw new IllegalArgumentException("Job ID should not be null");
        return "kylin_intermediate_" + dataModelDesc.getName() + "_stats_" + jobId.replace('-', '_');
    }

    @Override
    public NDataModel getDataModel() {
        return this.dataModelDesc;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columnList;
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            return -1;

        return index.intValue();
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
    public TblColRef getClusterBy() {
        return null;
    }

    @Override
    public ISegment getSegment() {
        return new EmptySegment();
    }

    class EmptySegment implements ISegment {
        @Override
        public KylinConfig getConfig() {
            return null;
        }
        
        @Override
        public String getName() {
            return null;
        }

        @Override
        public boolean isOffsetCube() {
            return false;
        }

        @Override
        public SegmentRange getSegRange() {
            return null;
        }

        @Override
        public TimeRange getTSRange() {
            return null;
        }

        @Override
        public NDataModel getModel() {
            return null;
        }

        @Override
        public SegmentStatusEnum getStatus() {
            return null;
        }

        @Override
        public long getLastBuildTime() {
            return 0;
        }

        @Override
        public void validate() {
            // Do nothing
        }

        @Override
        public int compareTo(ISegment o) {
            return 0;
        }

    }

}
