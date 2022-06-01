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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

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

    @JsonProperty("locked_index_count")
    private long lockedIndexCount;

    @JsonProperty("index_count_total")
    private long indexCountTotal;

    @JsonProperty("multi_partition_count")
    private long multiPartitionCount;

    @JsonProperty("multi_partition_count_total")
    private long multiPartitionCountTotal;

    @JsonProperty("source_count")
    private long sourceCount;

    @JsonProperty("row_count")
    private long rowCount;

    @JsonProperty("second_storage_nodes")
    private Map<String, List<SecondStorageNode>> secondStorageNodes;

    // byte
    @JsonProperty("second_storage_size")
    private long secondStorageSize;

    private long createTime;

    private long startTime;

    private long endTime;

    private long storage;

    @JsonProperty("has_base_table_index")
    private boolean hasBaseTableIndex;

    @JsonProperty("has_base_agg_index")
    private boolean hasBaseAggIndex;

    @JsonProperty("has_base_table_index_data")
    private boolean hasBaseTableIndexData;

    public NDataSegmentResponse() {
        super(false);
    }

    public NDataSegmentResponse(NDataflow dataflow, NDataSegment segment) {
        this(dataflow, segment, null);
    }

    public NDataSegmentResponse(NDataflow dataflow, NDataSegment segment, List<AbstractExecutable> executables) {
        super(segment);
        createTime = getCreateTimeUTC();
        startTime = Long.parseLong(getSegRange().getStart().toString());
        endTime = Long.parseLong(getSegRange().getEnd().toString());
        storage = bytesSize;
        indexCount = segment.getLayoutSize();
        indexCountTotal = segment.getIndexPlan().getAllLayoutsSize(true);
        multiPartitionCount = segment.getMultiPartitions().size();
        hasBaseAggIndex = segment.getIndexPlan().containBaseAggLayout();
        hasBaseTableIndex = segment.getIndexPlan().containBaseTableLayout();
        if (segment.getIndexPlan().getBaseTableLayout() != null) {
            val indexPlan = segment.getDataflow().getIndexPlan();
            long segmentFileCount = segment.getSegDetails().getLayouts().stream()
                    .filter(layout -> indexPlan.getLayoutEntity(layout.getLayoutId()).isBaseIndex())
                    .mapToLong(NDataLayout::getFileCount)
                    .sum();

            hasBaseTableIndexData = segmentFileCount > 0;
        } else {
            hasBaseTableIndexData = false;
        }
        lockedIndexCount = segment.getIndexPlan().getAllToBeDeleteLayoutId().stream().filter(layoutId ->
                segment.getLayoutsMap().containsKey(layoutId)).count();
        if (dataflow.getModel().getMultiPartitionDesc() != null) {
            multiPartitionCountTotal = dataflow.getModel().getMultiPartitionDesc().getPartitions().size();
        }
        sourceCount = segment.getSourceCount();
        rowCount = segment.getSegDetails().getTotalRowCount();
        setBytesSize(segment.getStorageBytesSize());
        getAdditionalInfo().put(SEGMENT_PATH, dataflow.getSegmentHdfsPath(segment.getId()));
        getAdditionalInfo().put(FILE_COUNT, segment.getStorageFileCount() + "");
        setStatusToDisplay(SegmentUtil.getSegmentStatusToDisplay(dataflow.getSegments(), segment, executables));
        setSourceBytesSize(segment.getSourceBytesSize());
        setLastBuildTime(segment.getLastBuildTime());
        setMaxBucketId(segment.getMaxBucketId());
        lastModifiedTime = getLastBuildTime() != 0 ? getLastBuildTime() : getCreateTime();
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

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

}