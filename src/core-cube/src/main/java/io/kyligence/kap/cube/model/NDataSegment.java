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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.NDictionaryInfo;
import org.apache.kylin.dict.NDictionaryManager;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TimeRange;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;

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

    @JsonProperty("dictionaries")
    private Map<String, String> dictionaries; // table/column ==> dictionary resource path
    @JsonProperty("snapshots")
    private Map<String, String> snapshots; // table name ==> snapshot resource path
    @JsonProperty("last_build_time")
    private long lastBuildTime; // last segment incr build job

    @JsonProperty("source_count")
    private long sourceCount = -1; // source table records number

    @JsonProperty("additionalInfo")
    private Map<String, String> additionalInfo = Maps.newLinkedHashMap();

    // computed fields below

    private transient NDataSegDetails segDetails; // transient, not required by spark cubing
    private transient Map<Long, NDataLayout> layoutsMap = Collections.emptyMap(); // transient, not required by spark cubing

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
        this.snapshots = other.snapshots;
        this.lastBuildTime = other.lastBuildTime;
        this.sourceCount = other.sourceCount;
        this.additionalInfo = other.additionalInfo;
    }

    void initAfterReload() {
        segDetails = NDataSegDetailsManager.getInstance(getConfig(), dataflow.getProject()).getForSegment(this);
        if (segDetails == null) {
            segDetails = NDataSegDetails.newSegDetails(dataflow, id);
        }

        segDetails.setCachedAndShared(dataflow.isCachedAndShared());

        List<NDataLayout> cuboids = segDetails.getLayouts();
        layoutsMap = new HashMap<>(cuboids.size());
        for (NDataLayout i : cuboids) {
            layoutsMap.put(i.getLayoutId(), i);
        }
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

    public void setTimeRange(TimeRange timeRange) {
        checkIsNotCachedAndShared();
        this.timeRange = timeRange;
    }

    public NDataSegDetails getSegDetails() {
        return segDetails;
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

    public IndexPlan getIndexPlan() {
        return dataflow.getIndexPlan();
    }

    @Override
    public void validate() {
        // Do nothing
    }

    public Map<TblColRef, Dictionary<String>> buildDictionaryMap() {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : getIndexPlan().getAllColumnsHaveDictionary()) {
            result.put(col, getDictionary(col));
        }
        return result;
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

    public Dictionary<String> getDictionary(TblColRef col) {
        IndexPlan indexPlan = getIndexPlan();
        TblColRef reuseCol = indexPlan.getDictionaryReuseColumn(col);
        NDictionaryInfo info = null;
        try {
            NDictionaryManager dictMgr = NDictionaryManager.getInstance(getConfig(), this.dataflow.getProject());
            String dictResPath = this.getDictResPath(reuseCol);
            if (dictResPath == null)
                return null;

            info = dictMgr.getDictionaryInfo(dictResPath);
            if (info == null)
                throw new IllegalStateException("No dictionary found by " + dictResPath
                        + ", invalid cube state; cube segment" + this + ", col " + reuseCol);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get dictionary for cube segment" + this + ", col" + reuseCol, e);
        }
        return info.getDictionaryObject();
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

    public Map<String, String> getSnapshots() {
        if (snapshots == null)
            snapshots = Maps.newConcurrentMap();

        return isCachedAndShared() ? ImmutableMap.copyOf(snapshots) : snapshots;
    }

    public void setSnapshots(Map<String, String> snapshots) {
        checkIsNotCachedAndShared();
        this.snapshots = snapshots;
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

    public long getSourceCount() {
        return sourceCount;
    }

    public void setSourceCount(long sourceCount) {
        checkIsNotCachedAndShared();
        this.sourceCount = sourceCount;
    }

    public String getProject() {
        return this.dataflow.getProject();
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

    public void putSnapshotResPath(String table, String snapshotResPath) {
        getSnapshots().put(table, snapshotResPath);
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
}
