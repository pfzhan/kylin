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

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataCuboid {

    public static NDataCuboid newDataCuboid(NDataSegDetails segDetails, long cuboidLayoutId) {
        NDataCuboid r = new NDataCuboid();
        r.setSegDetails(segDetails);
        r.setCuboidLayoutId(cuboidLayoutId);
        r.setStatus(SegmentStatusEnum.NEW);
        return r;
    }

    // ============================================================================

    @JsonBackReference
    private NDataSegDetails segDetails;
    @JsonProperty("cuboid_layout_id")
    private long cuboidLayoutId;
    @JsonProperty("status")
    private SegmentStatusEnum status;
    @JsonProperty("build_job_id")
    private String buildJobId;
    @JsonProperty("rows")
    private long rows;
    @JsonProperty("size_kb")
    private long sizeKB;
    @JsonProperty("file_count")
    private long fileCount;
    @JsonProperty("source_rows")
    private long sourceRows;
    @JsonProperty("source_kb")
    private long sourceKB;

    public NDataCuboid() {
    }

    public NDataSegDetails getSegDetails() {
        return segDetails;
    }

    public long getCuboidLayoutId() {
        return cuboidLayoutId;
    }

    public SegmentStatusEnum getStatus() {
        return status;
    }

    public String getBuildJobId() {
        return buildJobId;
    }

    public long getRows() {
        return rows;
    }

    public long getSizeKB() {
        return sizeKB;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public long getSourceKB() {
        return sourceKB;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setSegDetails(NDataSegDetails segDetails) {
        this.segDetails = segDetails;
    }

    public void setCuboidLayoutId(long cuboidLayoutId) {
        this.cuboidLayoutId = cuboidLayoutId;
    }

    public void setStatus(SegmentStatusEnum status) {
        this.status = status;
    }

    public void setBuildJobId(String buildJobId) {
        this.buildJobId = buildJobId;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public void setSizeKB(long sizeKB) {
        this.sizeKB = sizeKB;
    }

    public void setSourceRows(long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public void setSourceKB(long sourceKB) {
        this.sourceKB = sourceKB;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public KylinConfigExt getConfig() {
        return segDetails.getConfig();
    }

    public NCuboidLayout getCuboidLayout() {
        return segDetails.getDataflow().getCubePlan().getSpanningTree().getCuboidLayout(cuboidLayoutId);
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
        return "NDataCuboid [" + segDetails.getDataflowName() + "." + segDetails.getSegmentId() + "." + cuboidLayoutId
                + "]";
    }
}
