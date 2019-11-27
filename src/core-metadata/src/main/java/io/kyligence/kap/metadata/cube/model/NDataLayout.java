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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfigExt;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

import lombok.Getter;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataLayout implements Serializable {

    public static NDataLayout newDataLayout(NDataflow df, String segId, long layoutId) {
        return newDataLayout(NDataSegDetails.newSegDetails(df, segId), layoutId);
    }

    public static NDataLayout newDataLayout(NDataSegDetails segDetails, long layoutId) {
        NDataLayout r = new NDataLayout();
        r.setSegDetails(segDetails);
        r.setLayoutId(layoutId);
        return r;
    }

    // ============================================================================

    @JsonBackReference
    private NDataSegDetails segDetails;
    @JsonProperty("layout_id")
    private long layoutId;
    @JsonProperty("build_job_id")
    private String buildJobId;
    @JsonProperty("rows")
    private long rows;
    @JsonProperty("byte_size")
    private long byteSize;
    @JsonProperty("file_count")
    private long fileCount;
    @JsonProperty("source_rows")
    private long sourceRows;
    @JsonProperty("source_byte_size")
    private long sourceByteSize;
    // partition num may be diff with file num
    @JsonProperty("partition_num")
    private int partitionNum;

    @JsonProperty("partition_values")
    private List<String> partitionValues = new ArrayList<>();

    @Getter
    @JsonProperty("create_time")
    private long createTime;

    public NDataLayout() {
        this.createTime = System.currentTimeMillis();
    }

    public KylinConfigExt getConfig() {
        return segDetails.getConfig();
    }

    public LayoutEntity getLayout() {
        return segDetails.getDataflow().getIndexPlan().getSpanningTree().getCuboidLayout(layoutId);
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NDataSegDetails getSegDetails() {
        return segDetails;
    }

    public void setSegDetails(NDataSegDetails segDetails) {
        checkIsNotCachedAndShared();
        this.segDetails = segDetails;
    }

    public long getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(long layoutId) {
        checkIsNotCachedAndShared();
        this.layoutId = layoutId;
    }

    public String getBuildJobId() {
        return buildJobId;
    }

    public void setBuildJobId(String buildJobId) {
        checkIsNotCachedAndShared();
        this.buildJobId = buildJobId;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        checkIsNotCachedAndShared();
        this.rows = rows;
    }

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        checkIsNotCachedAndShared();
        this.byteSize = byteSize;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        checkIsNotCachedAndShared();
        this.sourceRows = sourceRows;
    }

    public long getSourceByteSize() {
        return sourceByteSize;
    }

    public void setSourceByteSize(long sourceByteSize) {
        checkIsNotCachedAndShared();
        this.sourceByteSize = sourceByteSize;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        checkIsNotCachedAndShared();
        this.fileCount = fileCount;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        checkIsNotCachedAndShared();
        this.partitionNum = partitionNum;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(List<String> partitionValues) {
        this.partitionValues = partitionValues;
    }

    // ============================================================================

    public boolean isCachedAndShared() {
        if (segDetails == null || segDetails.isCachedAndShared() == false)
            return false;

        for (NDataLayout cached : segDetails.getLayouts()) {
            if (cached == this)
                return true;
        }
        return false;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public long getIndexId() {
        return (this.getLayoutId() / IndexEntity.INDEX_ID_STEP) * IndexEntity.INDEX_ID_STEP;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Longs.hashCode(layoutId);
        result = prime * result + ((segDetails == null) ? 0 : segDetails.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NDataLayout other = (NDataLayout) obj;
        if (layoutId != other.layoutId)
            return false;
        if (segDetails == null) {
            if (other.segDetails != null)
                return false;
        } else if (!segDetails.equals(other.segDetails))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "NDataLayout [ Model Name:" + segDetails.getDataflow().getModelAlias() + ", Segment Id:" + segDetails.getId()
                + ", Layout Id:" + layoutId + "]";
    }
}
