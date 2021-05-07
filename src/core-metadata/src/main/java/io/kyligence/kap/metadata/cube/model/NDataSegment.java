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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TimeRange;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.MultiPartitionUtil;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataSegment implements ISegment, Serializable, IKeep {

    @JsonBackReference
    private NDataflow dataflow;
    @JsonProperty("id")
    private String id; // Sequence ID within NDataflow
    @JsonProperty("name")
    private String name;
    @JsonProperty("create_time_utc")
    private long createTimeUTC;
    @JsonProperty("status")
    private SegmentStatusEnum status;

    @JsonProperty("segRange")
    private SegmentRange segmentRange;

    @JsonProperty("timeRange")
    private TimeRange timeRange;

    @JsonProperty("dimension_range_info_map")
    private Map<String, DimensionRangeInfo> dimensionRangeInfoMap = Maps.newHashMap();

    @JsonProperty("parameters")
    private Map<String, String> parameters;

    @JsonProperty("dictionaries")
    private Map<String, String> dictionaries; // table/column ==> dictionary resource path
    @JsonProperty("snapshots")
    private Map<String, String> snapshots; // table name ==> snapshot resource path
    @JsonProperty("last_build_time")
    private long lastBuildTime; // last segment incr build job

    @JsonProperty("source_count")
    private long sourceCount = 0; // source table records number

    @JsonProperty("source_bytes_size")
    private long sourceBytesSize = 0;

    @JsonProperty("column_source_bytes")
    private Map<String, Long> columnSourceBytes = Maps.newHashMap();

    @Getter
    @Setter
    @JsonProperty("ori_snapshot_size")
    private Map<String, Long> oriSnapshotSize = Maps.newHashMap();

    private Long storageSize = null;

    private Long storageFileCount = null;

    @JsonProperty("additionalInfo")
    private Map<String, String> additionalInfo = Maps.newLinkedHashMap();

    @JsonProperty("is_encoding_data_skew")
    private boolean isEncodingDataSkew = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_snapshot_ready")
    private boolean isSnapshotReady = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_dict_ready")
    private boolean isDictReady = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_flat_table_ready")
    private boolean isFlatTableReady = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_fact_view_ready")
    private boolean isFactViewReady = false;

    @JsonProperty("multi_partitions")
    private List<SegmentPartition> multiPartitions = Lists.newArrayList();

    @JsonProperty("max_bucket_id")
    @Getter
    private long maxBucketId = -1L;

    private transient Set<String> excludedTables = Sets.newHashSet();

    // computed fields below
    // transient, not required by spark cubing
    private transient NDataSegDetails segDetails;
    // transient, not required by spark cubing
    private transient Map<Long, NDataLayout> layoutsMap = Collections.emptyMap();
    // transient,generated by multiPartitionData
    @Getter
    private transient Map<Long, SegmentPartition> partitionMap = Maps.newHashMap();
    /**
     * for each layout, partition id -> bucket id
     */
    private transient Map<Long, Map<Long, Long>> partitionBucketMap = Maps.newHashMap();

    public NDataSegment() {
    }

    public NDataSegment(NDataSegment other) {
        this.id = other.id;
        this.name = other.name;
        this.createTimeUTC = other.createTimeUTC;
        this.status = other.status;
        this.segmentRange = other.segmentRange;
        this.timeRange = other.timeRange;
        this.dictionaries = other.dictionaries;
        this.lastBuildTime = other.lastBuildTime;
        this.sourceCount = other.sourceCount;
        this.additionalInfo = other.additionalInfo;
        this.excludedTables = other.excludedTables;
        this.isEncodingDataSkew = other.isEncodingDataSkew;
        this.isSnapshotReady = other.isSnapshotReady;
        this.isDictReady = other.isDictReady;
        this.isFlatTableReady = other.isFlatTableReady;
        this.isFactViewReady = other.isFactViewReady;
    }

    void initAfterReload() {
        segDetails = NDataSegDetailsManager.getInstance(getConfig(), dataflow.getProject()).getForSegment(this);
        if (segDetails == null) {
            segDetails = NDataSegDetails.newSegDetails(dataflow, id);
        }

        segDetails.setCachedAndShared(dataflow.isCachedAndShared());

        List<NDataLayout> cuboids = segDetails.getLayouts();
        layoutsMap = new HashMap<>(cuboids.size());
        this.multiPartitions.forEach(partition -> {
            partition.setSegment(this);
            partitionMap.put(partition.getPartitionId(), partition);
        });
        for (NDataLayout cuboid : cuboids) {
            layoutsMap.put(cuboid.getLayoutId(), cuboid);
            Map<Long, Long> cuboidBucketMap = Maps.newHashMap();
            cuboid.getMultiPartition().forEach(
                    dataPartition -> cuboidBucketMap.put(dataPartition.getPartitionId(), dataPartition.getBucketId()));
            partitionBucketMap.put(cuboid.getLayoutId(), cuboidBucketMap);
        }
    }

    public void setLayoutsMap(Map<Long, NDataLayout> lym) {
        this.layoutsMap = lym;
    }

    @Override
    public KylinConfig getConfig() {
        return dataflow.getConfig();
    }

    @Override
    public boolean isOffsetCube() {
        return segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange;
    }

    @Override
    public SegmentRange getSegRange() {
        return segmentRange;
    }

    @Override
    public SegmentRange.KafkaOffsetPartitionedSegmentRange getKSRange() {
        if (segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange) {
            return (SegmentRange.KafkaOffsetPartitionedSegmentRange) segmentRange;
        }
        return null;
    }

    public Map<String, DimensionRangeInfo> getDimensionRangeInfoMap() {
        return dimensionRangeInfoMap;
    }

    @Override
    public TimeRange getTSRange() {
        if (timeRange != null) {
            return timeRange;
        } else if (segmentRange instanceof SegmentRange.TimePartitionedSegmentRange) {
            SegmentRange.TimePartitionedSegmentRange tsr = (SegmentRange.TimePartitionedSegmentRange) segmentRange;
            return new TimeRange(tsr.getStart(), tsr.getEnd());
        }
        return null;
    }

    public void setSegmentRange(SegmentRange segmentRange) {
        checkIsNotCachedAndShared();
        this.segmentRange = segmentRange;
    }

    public void setDimensionRangeInfoMap(Map<String, DimensionRangeInfo> dimensionRangeInfoMap) {
        checkIsNotCachedAndShared();
        this.dimensionRangeInfoMap = dimensionRangeInfoMap;
    }

    public void setTimeRange(TimeRange timeRange) {
        checkIsNotCachedAndShared();
        this.timeRange = timeRange;
    }

    public void setSegDetails(NDataSegDetails segDetails) {
        checkIsNotCachedAndShared();
        this.segDetails = segDetails;
    }

    public NDataSegDetails getSegDetails() {
        return segDetails;
    }

    @Override
    public int getLayoutSize() {
        return layoutsMap.size();
    }

    @Override
    public NDataModel getModel() {
        return dataflow.getModel();
    }

    public NDataLayout getLayout(long layoutId) {
        return layoutsMap.get(layoutId);
    }

    public Map<Long, NDataLayout> getLayoutsMap() {
        return layoutsMap;
    }

    public Set<Long> getLayoutIds() {
        return layoutsMap.keySet();
    }

    public IndexPlan getIndexPlan() {
        return dataflow.getIndexPlan();
    }

    @Override
    public void validate() {
        // Do nothing
    }

    public String getDictResPath(TblColRef col) {
        String r;
        String dictKey = col.getIdentity();
        r = getDictionaries().get(dictKey);

        // try Kylin v1.x dict key as well
        if (r == null) {
            String v1DictKey = col.getTable() + "/" + col.getName();
            r = getDictionaries().get(v1DictKey);
        }

        return r;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NDataflow getDataflow() {
        return dataflow;
    }

    public void setDataflow(NDataflow df) {
        checkIsNotCachedAndShared();
        this.dataflow = df;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        checkIsNotCachedAndShared();
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        checkIsNotCachedAndShared();
        this.name = name;
    }

    @Override
    public SegmentStatusEnum getStatus() {
        return status;
    }

    public void setStatus(SegmentStatusEnum status) {
        checkIsNotCachedAndShared();
        this.status = status;
    }

    @Override
    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        checkIsNotCachedAndShared();
        this.lastBuildTime = lastBuildTime;
    }

    public Map<String, String> getParameters() {
        if (parameters == null)
            parameters = Maps.newConcurrentMap();

        return isCachedAndShared() ? ImmutableMap.copyOf(parameters) : parameters;
    }

    public String getParameter(String key) {
        if (parameters == null) {
            return null;
        } else {
            return parameters.get(key);
        }
    }

    public void setParameters(Map<String, String> parameters) {
        checkIsNotCachedAndShared();
        this.parameters = parameters;
    }

    public void addParameter(String key, String value) {
        checkIsNotCachedAndShared();
        if (parameters == null)
            parameters = Maps.newConcurrentMap();
        parameters.put(key, value);
    }

    public Map<String, String> getDictionaries() {
        if (dictionaries == null)
            dictionaries = Maps.newConcurrentMap();

        return isCachedAndShared() ? ImmutableMap.copyOf(dictionaries) : dictionaries;
    }

    public void putDictResPath(TblColRef col, String dictResPath) {
        checkIsNotCachedAndShared();
        getDictionaries(); // touch to create
        String dictKey = col.getIdentity();
        dictionaries.put(dictKey, dictResPath);
    }

    public void setDictionaries(Map<String, String> dictionaries) {
        checkIsNotCachedAndShared();
        this.dictionaries = dictionaries;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        checkIsNotCachedAndShared();
        this.createTimeUTC = createTimeUTC;
    }

    public Map<String, String> getAdditionalInfo() {
        return isCachedAndShared() ? ImmutableMap.copyOf(additionalInfo) : additionalInfo;
    }

    public void setAdditionalInfo(Map<String, String> additionalInfo) {
        checkIsNotCachedAndShared();
        this.additionalInfo = additionalInfo;
    }

    public Set<String> getExcludedTables() {
        return excludedTables;
    }

    public void setExcludedTables(Set<String> excludedTables) {
        this.excludedTables = excludedTables;
    }

    public long getSourceCount() {
        if (CollectionUtils.isEmpty(multiPartitions)) {
            return sourceCount;
        }
        return multiPartitions.stream().mapToLong(SegmentPartition::getSourceCount).sum();
    }

    public void setSourceCount(long sourceCount) {
        checkIsNotCachedAndShared();
        this.sourceCount = sourceCount;
    }

    public long getSourceBytesSize() {
        return this.sourceBytesSize;
    }

    public void setSourceBytesSize(long sourceBytesSize) {
        checkIsNotCachedAndShared();
        this.sourceBytesSize = sourceBytesSize;
    }

    public Map<String, Long> getColumnSourceBytes() {
        return columnSourceBytes;
    }

    public void setColumnSourceBytes(Map<String, Long> columnSourceBytes) {
        this.columnSourceBytes = columnSourceBytes;
    }

    public boolean isEncodingDataSkew() {
        return isEncodingDataSkew;
    }

    public void setEncodingDataSkew(boolean encodingDataSkew) {
        isEncodingDataSkew = encodingDataSkew;
    }

    public String getProject() {
        return this.dataflow.getProject();
    }

    public List<SegmentPartition> getMultiPartitions() {
        return multiPartitions;
    }

    public void setMultiPartitions(List<SegmentPartition> multiPartitions) {
        checkIsNotCachedAndShared();
        this.multiPartitions = multiPartitions;
    }

    public Set<Long> getAllPartitionIds() {
        return multiPartitions.stream().map(SegmentPartition::getPartitionId).collect(Collectors.toSet());
    }

    // ============================================================================

    public boolean isCachedAndShared() {
        if (dataflow == null || !dataflow.isCachedAndShared())
            return false;

        for (NDataSegment cached : dataflow.getSegments()) {
            if (cached == this)
                return true;
        }
        return false;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public long getStorageBytesSize() {
        if (storageSize == null) {
            long size = 0;
            Collection<NDataLayout> dataLayouts = getLayoutsMap().values();
            for (NDataLayout dataLayout : dataLayouts) {
                size += dataLayout.getByteSize();
            }
            storageSize = size;
        }
        return storageSize;
    }

    public long getStorageFileCount() {
        if (storageFileCount == null) {
            long fileCount = 0L;
            Collection<NDataLayout> dataLayouts = getLayoutsMap().values();
            for (NDataLayout dataLayout : dataLayouts) {
                fileCount += dataLayout.getFileCount();
            }
            storageFileCount = fileCount;
        }
        return storageFileCount;
    }

    public boolean isAlreadyBuilt(long layoutId) {
        if (Objects.nonNull(layoutsMap) && layoutsMap.containsKey(layoutId)) {
            return true;
        }
        return false;
    }

    public boolean isSnapshotReady() {
        return isSnapshotReady;
    }

    public void setSnapshotReady(boolean snapshotReady) {
        isSnapshotReady = snapshotReady;
    }

    public boolean isDictReady() {
        return isDictReady;
    }

    public void setDictReady(boolean dictReady) {
        isDictReady = dictReady;
    }

    public boolean isFlatTableReady() {
        return isFlatTableReady;
    }

    public void setFlatTableReady(boolean flatTableReady) {
        isFlatTableReady = flatTableReady;
    }

    public boolean isFactViewReady() {
        return isFactViewReady;
    }

    public void setFactViewReady(boolean factViewReady) {
        isFactViewReady = factViewReady;
    }

    public List<Long> getMultiPartitionIds() {
        return partitionBucketMap.values().stream().map(entry -> entry.keySet()).flatMap(Set::stream).distinct()
                .collect(Collectors.toList());
    }

    public Long getBucketId(long cuboidId, Long partitionId) {
        Map<Long, Long> cuboidBucketMap = partitionBucketMap.get(cuboidId);
        if (cuboidBucketMap == null)
            return null;
        return cuboidBucketMap.get(partitionId);
    }

    public SegmentPartition getPartition(long partitionId) {
        return partitionMap.get(partitionId);
    }

    public boolean isPartitionOverlap(String[] value) {
        List<String[]> newValues = Lists.newArrayList();
        newValues.add(value);
        return !findDuplicatePartitions(newValues).isEmpty();
    }

    public List<String[]> findDuplicatePartitions(List<String[]> newValues) {
        NDataModel model = this.getModel();
        Preconditions.checkState(model.isMultiPartitionModel());
        List<Long> oldPartitionIds = this.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                .collect(Collectors.toList());
        List<String[]> oldPartitionValues = model.getMultiPartitionDesc().getPartitionValuesById(oldPartitionIds);
        return MultiPartitionUtil.findDuplicateValues(oldPartitionValues, newValues);
    }

    public long increaseBucket(long step) {
        checkIsNotCachedAndShared();
        return this.maxBucketId += step;
    }

    public void setMaxBucketId(long maxBucketId) {
        checkIsNotCachedAndShared();
        this.maxBucketId = maxBucketId;
    }

    @Override
    public int compareTo(ISegment other) {
        SegmentRange<?> x = this.getSegRange();
        return x.compareTo(other.getSegRange());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataflow == null) ? 0 : dataflow.hashCode());
        result = prime * result + id.hashCode();
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
        NDataSegment other = (NDataSegment) obj;
        if (dataflow == null) {
            if (other.dataflow != null)
                return false;
        } else if (!dataflow.equals(other.dataflow))
            return false;
        return id.equals(other.id);
    }

    @Override
    public String toString() {
        return "NDataSegment [" + dataflow.getUuid() + "," + id + "," + segmentRange + "]";
    }

    public String displayIdName() {
        return String.format(Locale.ROOT, "[id:%s,name:%s]", id, name);
    }
}
