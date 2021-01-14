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

package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class NDataSegmentResponse extends NDataSegment {

    private static final String SEGMENT_PATH = "segment_path";

    private static final String FILE_COUNT = "file_count";

    @JsonProperty("bytes_size")
    private long bytesSize;

    @JsonProperty("hit_count")
    private long hitCount;

    @JsonProperty("source_bytes_size")
    private long sourceBytesSize;

    @JsonProperty("status_to_display")
    private SegmentStatusEnumToDisplay statusToDisplay;

    @JsonProperty("index_count")
    private long indexCount;

    @JsonProperty("index_count_total")
    private long indexCountTotal;

    @JsonProperty("multi_partition_count")
    private long multiPartitionCount;

    @JsonProperty("multi_partition_count_total")
    private long multiPartitionCountTotal;

    @JsonProperty("row_count")
    private long rowCount;

    private long createTime;

    private long startTime;

    private long endTime;

    private long storage;

    public NDataSegmentResponse() {
        super();
    }

    public NDataSegmentResponse(NDataflow dataflow, NDataSegment segment) {
        super(segment);
        createTime = getCreateTimeUTC();
        startTime = Long.parseLong(getSegRange().getStart().toString());
        endTime = Long.parseLong(getSegRange().getEnd().toString());
        storage = bytesSize;
        indexCount = segment.getLayoutSize();
        indexCountTotal = segment.getIndexPlan().getAllLayouts().size();
        multiPartitionCount = segment.getMultiPartitions().size();
        if (dataflow.getModel().getMultiPartitionDesc() != null) {
            multiPartitionCountTotal = dataflow.getModel().getMultiPartitionDesc().getPartitions().size();
        }
        rowCount = segment.getSegDetails().getTotalRowCount();
        setBytesSize(segment.getStorageBytesSize());
        getAdditionalInfo().put(SEGMENT_PATH, dataflow.getSegmentHdfsPath(segment.getId()));
        getAdditionalInfo().put(FILE_COUNT, segment.getStorageFileCount() + "");
        setStatusToDisplay(SegmentUtil.getSegmentStatusToDisplay(dataflow.getSegments(), segment));
        setSourceBytesSize(segment.getSourceBytesSize());
        setLastBuildTime(segment.getLastBuildTime());
        setSegDetails(segment.getSegDetails());
        setMaxBucketId(segment.getMaxBucketId());
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    @Getter
    @Setter
    private OldParams oldParams;

    @Getter
    @Setter
    public static class OldParams implements Serializable {
        @JsonProperty("size_kb")
        private long sizeKB;

        @JsonProperty("input_records")
        private long inputRecords;
    }

    @JsonGetter("last_modified_time")
    public long lastModifiedTime() {
        return getLastBuildTime() != 0 ? getLastBuildTime() : getCreateTime();
    }

}