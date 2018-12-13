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

import org.apache.kylin.common.KylinConfigExt;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

import lombok.Getter;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataCuboid implements Serializable {

    public static NDataCuboid newDataCuboid(NDataflow df, String segId, long cuboidLayoutId) {
        return newDataCuboid(NDataSegDetails.newSegDetails(df, segId), cuboidLayoutId);
    }

    public static NDataCuboid newDataCuboid(NDataSegDetails segDetails, long cuboidLayoutId) {
        NDataCuboid r = new NDataCuboid();
        r.setSegDetails(segDetails);
        r.setCuboidLayoutId(cuboidLayoutId);
        return r;
    }

    // ============================================================================

    @JsonBackReference
    private NDataSegDetails segDetails;
    @JsonProperty("cuboid_layout_id")
    private long cuboidLayoutId;
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
    @Getter
    @JsonProperty("create_time")
    private long createTime;

    public NDataCuboid() {
        this.createTime = System.currentTimeMillis();
    }

    public KylinConfigExt getConfig() {
        return segDetails.getConfig();
    }

    public NCuboidLayout getCuboidLayout() {
        return segDetails.getDataflow().getCubePlan().getSpanningTree().getCuboidLayout(cuboidLayoutId);
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

    public long getCuboidLayoutId() {
        return cuboidLayoutId;
    }

    public void setCuboidLayoutId(long cuboidLayoutId) {
        checkIsNotCachedAndShared();
        this.cuboidLayoutId = cuboidLayoutId;
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

    // ============================================================================

    public boolean isCachedAndShared() {
        if (segDetails == null || segDetails.isCachedAndShared() == false)
            return false;

        for (NDataCuboid cached : segDetails.getCuboids()) {
            if (cached == this)
                return true;
        }
        return false;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public long getCuboidDescId() {
        return (this.getCuboidLayoutId() / NCuboidDesc.CUBOID_DESC_ID_STEP) * NCuboidDesc.CUBOID_DESC_ID_STEP;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Longs.hashCode(cuboidLayoutId);
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
        NDataCuboid other = (NDataCuboid) obj;
        if (cuboidLayoutId != other.cuboidLayoutId)
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
        return "NDataCuboid [" + segDetails.getDataflowName() + "." + segDetails.getUuid() + "." + cuboidLayoutId + "]";
    }
}
