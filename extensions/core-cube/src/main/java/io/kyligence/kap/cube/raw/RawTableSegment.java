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

package io.kyligence.kap.cube.raw;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * RawTableSegment has a 1-1 relationship to CubeSegment. Their linkage is the identical 'uuid' attribute.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableSegment implements Comparable<RawTableSegment>, IBuildable, ISegment {

    @JsonBackReference
    private RawTableInstance rawTableInstance;
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("date_range_start")
    private long dateRangeStart;
    @JsonProperty("date_range_end")
    private long dateRangeEnd;
    @JsonProperty("source_offset_start")
    private long sourceOffsetStart;
    @JsonProperty("source_offset_end")
    private long sourceOffsetEnd;
    @JsonProperty("status")
    private SegmentStatusEnum status;
    @JsonProperty("size_kb")
    private long sizeKB;
    @JsonProperty("input_records")
    private long inputRecords;
    @JsonProperty("input_records_size")
    private long inputRecordsSize;
    @JsonProperty("last_build_time")
    private long lastBuildTime;
    @JsonProperty("last_build_job_id")
    private String lastBuildJobID;
    @JsonProperty("create_time_utc")
    private long createTimeUTC;
    @JsonProperty("index_path")
    private String indexPath;
    @JsonProperty("shard_number")
    private int shardNumber = 10;

    public RawTableSegment() {
    }

    public RawTableSegment(RawTableInstance rawTable) {
        this.rawTableInstance = rawTable;
    }

    public void setShardNum(int num) {
        this.shardNumber = num;
    }

    public int getShardNum() {
        return this.shardNumber;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public RawTableInstance getRawTableInstance() {
        return rawTableInstance;
    }

    public void setStatus(SegmentStatusEnum status) {
        this.status = status;
    }

    public String getLastBuildJobID() {
        return lastBuildJobID;
    }

    public void setLastBuildJobID(String lastBuildJobID) {
        this.lastBuildJobID = lastBuildJobID;
    }

    public String getStatisticsResourcePath() {
        return getStatisticsResourcePath(this.rawTableInstance.getName(), this.getUuid());
    }

    public static String getStatisticsResourcePath(String cubeName, String cubeSegmentId) {
        return "/rawtable_statistics" + "/" + cubeName + "/" + cubeSegmentId + ".seq";
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String id) {
        this.uuid = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getDateRangeStart() {
        return dateRangeStart;
    }

    public void setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    public void setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }

    public SegmentStatusEnum getStatus() {
        return status;
    }

    // date range is used in place of source offsets when offsets are missing
    public long getSourceOffsetEnd() {
        return isSourceOffsetsOn() ? sourceOffsetEnd : dateRangeEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    public boolean isSourceOffsetsOn() {
        return sourceOffsetStart != 0 || sourceOffsetEnd != 0;
    }

    // date range is used in place of source offsets when offsets are missing

    public boolean sourceOffsetContains(RawTableSegment seg) {
        return Segments.sourceOffsetContains(this, seg);
    }

    public boolean dateRangeContains(RawTableSegment seg) {
        return dateRangeStart <= seg.dateRangeStart && seg.dateRangeEnd <= dateRangeEnd;
    }

    // date range is used in place of source offsets when offsets are missing
    public long getSourceOffsetStart() {
        return isSourceOffsetsOn() ? sourceOffsetStart : dateRangeStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    public KylinConfig getConfig() {
        return rawTableInstance.getConfig();
    }

    public void validate() {
        if (rawTableInstance.getRawTableDesc().getModel().getPartitionDesc().isPartitioned()) {
            if (!isSourceOffsetsOn() && dateRangeStart >= dateRangeEnd)
                throw new IllegalStateException("Invalid segment, dateRangeStart(" + dateRangeStart
                        + ") must be smaller than dateRangeEnd(" + dateRangeEnd + ") in segment " + this);
            if (isSourceOffsetsOn() && sourceOffsetStart >= sourceOffsetEnd)
                throw new IllegalStateException("Invalid segment, sourceOffsetStart(" + sourceOffsetStart
                        + ") must be smaller than sourceOffsetEnd(" + sourceOffsetEnd + ") in segment " + this);
        }
    }

    public boolean sourceOffsetOverlaps(RawTableSegment seg) {
        if (isSourceOffsetsOn())
            return sourceOffsetStart < seg.sourceOffsetEnd && seg.sourceOffsetStart < sourceOffsetEnd;
        else
            return dateRangeOverlaps(seg);
    }

    public boolean dateRangeOverlaps(RawTableSegment seg) {
        return dateRangeStart < seg.dateRangeEnd && seg.dateRangeStart < dateRangeEnd;
    }

    @Override
    public DataModelDesc getModel() {
        return this.getRawTableInstance().getRawTableDesc().getModel();
    }

    public CubeSegment getCubeSegment() {
        CubeSegment cubeSeg = rawTableInstance.getCubeInstance().getSegmentById(uuid);
        if (cubeSeg == null)
            throw new IllegalStateException(
                    "Cannot find the cube segment that this raw table segment attaches to: " + this + ", uuid=" + uuid);
        return cubeSeg;
    }

    @Override
    public int getStorageType() {
        return rawTableInstance.getStorageType();
    }

    @Override
    public int compareTo(RawTableSegment other) {
        long comp = this.getSourceOffsetStart() - other.getSourceOffsetStart();
        if (comp != 0)
            return comp < 0 ? -1 : 1;

        comp = this.getSourceOffsetEnd() - other.getSourceOffsetEnd();
        if (comp != 0)
            return comp < 0 ? -1 : 1;
        else
            return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((rawTableInstance == null) ? 0 : rawTableInstance.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
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
        RawTableSegment other = (RawTableSegment) obj;
        if (rawTableInstance == null) {
            if (other.rawTableInstance != null)
                return false;
        } else if (!rawTableInstance.equals(other.rawTableInstance))
            return false;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (status != other.status)
            return false;
        return true;
    }

    public static String makeSegmentName(long startDate, long endDate, long startOffset, long endOffset) {
        if (startOffset != 0 || endOffset != 0) {
            if (startOffset == 0 && (endOffset == 0 || endOffset == Long.MAX_VALUE)) {
                return "FULL_BUILD";
            }

            return startOffset + "_" + endOffset;
        }

        // using time
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(startDate) + "_" + dateFormat.format(endDate);
    }

    public long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public long getInputRecordsSize() {
        return inputRecordsSize;
    }

    public void setInputRecordsSize(long inputRecordsSize) {
        this.inputRecordsSize = inputRecordsSize;
    }

    public long getSizeKB() {
        return sizeKB;
    }

    public void setSizeKB(long sizeKB) {
        this.sizeKB = sizeKB;
    }

    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    @Override
    public int getEngineType() {
        return rawTableInstance.getRawTableDesc().getEngineType();
    }

    @Override
    public int getSourceType() {
        return rawTableInstance.getRawTableDesc().getStorageType();
    }

    @Override
    public String toString() {
        return rawTableInstance.getName() + "[" + name + "]";
    }

}
