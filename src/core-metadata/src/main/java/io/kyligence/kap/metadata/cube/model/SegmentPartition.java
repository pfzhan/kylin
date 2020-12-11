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
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import lombok.Getter;

/**
 * Partitions build job information in segment, almost like {@link NDataSegment}
 */
@SuppressWarnings("serial")
@Getter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SegmentPartition implements Serializable, IKeep {

    @JsonBackReference
    private NDataSegment segment;

    @JsonProperty("partition_id")
    private long partitionId;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("status")
    private PartitionStatusEnum status;

    // last partition incr build job
    @JsonProperty("last_build_time")
    private long lastBuildTime;

    // source table records number
    @JsonProperty("source_count")
    private long sourceCount = -1;

    @JsonProperty("column_source_bytes")
    private Map<String, Long> columnSourceBytes = Maps.newHashMap();

    private long storageSize = -1;

    // ============================================================================
    public SegmentPartition() {
    }

    public SegmentPartition(long partitionId) {
        this.partitionId = partitionId;
        this.status = PartitionStatusEnum.NEW;
        this.setCreateTimeUTC(System.currentTimeMillis());
    }

    public void setSegment(NDataSegment segment) {
        checkIsNotCachedAndShared();
        this.segment = segment;
    }

    public void setPartitionId(long partitionId) {
        checkIsNotCachedAndShared();
        this.partitionId = partitionId;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        checkIsNotCachedAndShared();
        this.createTimeUTC = createTimeUTC;
    }

    public void setStatus(PartitionStatusEnum status) {
        checkIsNotCachedAndShared();
        this.status = status;
    }

    public void setLastBuildTime(long lastBuildTime) {
        checkIsNotCachedAndShared();
        this.lastBuildTime = lastBuildTime;
    }

    public void setSourceCount(long sourceCount) {
        checkIsNotCachedAndShared();
        this.sourceCount = sourceCount;
    }

    public void setColumnSourceBytes(Map<String, Long> columnSourceBytes) {
        checkIsNotCachedAndShared();
        this.columnSourceBytes = columnSourceBytes;
    }

    public void setStorageSize(Long storageSize) {
        checkIsNotCachedAndShared();
        this.storageSize = storageSize;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public boolean isCachedAndShared() {
        if (segment == null || !segment.isCachedAndShared())
            return false;

        for (SegmentPartition partition : segment.getMultiPartitions()) {
            if (partition == this)
                return true;
        }
        return false;
    }

    public long getSourceCount() {
        if (sourceCount == -1) {
            return 0L;
        }
        return sourceCount;
    }

    public long getStorageSize() {
        if (storageSize == -1) {
            final NDataSegment dataSegment = getSegment();
            if (Objects.isNull(dataSegment)) {
                return 0;
            }
            storageSize = dataSegment.getSegDetails() //
                    .getLayouts().stream() //
                    .flatMap(layout -> layout.getMultiPartition().stream()) //
                    .filter(partition -> partition.getPartitionId() == partitionId) //
                    .mapToLong(LayoutPartition::getByteSize).sum();
        }
        return storageSize;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((segment == null) ? 0 : segment.hashCode());
        result = prime * result + Long.valueOf(partitionId).hashCode();
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
        SegmentPartition other = (SegmentPartition) obj;
        if (this.segment == null) {
            if (other.segment != null)
                return false;
        } else if (!segment.equals(other.segment))
            return false;
        return partitionId == other.partitionId;
    }
}
