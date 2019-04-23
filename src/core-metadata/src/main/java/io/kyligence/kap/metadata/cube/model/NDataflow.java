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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataflow extends RootPersistentEntity implements Serializable, IRealization, IKeep {
    public static final String REALIZATION_TYPE = "NCUBE";
    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";

    public static NDataflow create(IndexPlan plan) {
        NDataflow df = new NDataflow();

        df.config = (KylinConfigExt) plan.getConfig();
        df.setUuid(plan.getUuid());
        df.setCreateTimeUTC(System.currentTimeMillis());
        df.setSegments(new Segments<>());
        df.setStatus(RealizationStatusEnum.ONLINE);

        return df;
    }

    public static String concatResourcePath(String name) {
        return DATAFLOW_RESOURCE_ROOT + "/" + name + ".json";
    }

    // ============================================================================

    @JsonIgnore
    @Setter
    private KylinConfigExt config;
    @JsonProperty("description")
    private String description;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("mp_values")
    private String[] mpValues = StringUtil.EMPTY_ARRAY;
    @JsonProperty("create_time_utc")
    private long createTimeUTC;
    @JsonProperty("status")
    private RealizationStatusEnum status;
    @JsonProperty("cost")
    private int cost = 50;

    @Getter
    @Setter
    @JsonProperty("query_hit_count")
    private int queryHitCount = 0;

    @Getter
    @Setter
    @JsonProperty("event_error")
    private boolean eventError;

    @JsonManagedReference
    @JsonProperty("segments")
    private Segments<NDataSegment> segments = new Segments<NDataSegment>();

    @JsonProperty("storage_location_identifier")
    private String storageLocationIdentifier; // maybe useful in some cases..

    @Setter
    private String project;

    // ================================================================

    public void initAfterReload(KylinConfigExt config, String project) {
        this.project = project;
        this.config = config;
        for (NDataSegment seg : segments) {
            seg.initAfterReload();
        }
    }

    public KylinConfigExt getConfig() {
        return (KylinConfigExt) getIndexPlan().getConfig();
    }

    public NDataflow copy() {
        return NDataflowManager.getInstance(config, project).copy(this);
    }

    @Override
    public String resourceName() {
        return uuid;
    }

    public String getResourcePath() {
        return "/" + project + DATAFLOW_RESOURCE_ROOT + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // dataflow & segments
        r.add(this.getResourcePath());
        for (NDataSegment seg : segments) {
            r.add(seg.getSegDetails().getResourcePath());
        }

        // cubing plan
        r.add(getIndexPlan().getResourcePath());

        // project & model & tables
        r.add(getModel().getProjectInstance().getResourcePath());
        r.add(getModel().getResourcePath());
        for (TableRef t : getModel().getAllTables()) {
            r.add(t.getTableDesc().getResourcePath());
        }

        return r;
    }

    public IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(config, project).getIndexPlan(uuid);
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {

        return NDataflowCapabilityChecker.check(this, digest);
    }

    @Override
    public String getType() {
        return REALIZATION_TYPE;
    }

    @Override
    public NDataModel getModel() {
        return NDataModelManager.getInstance(config, project).getDataModelDesc(uuid);
    }

    public String getModelAlias() {
        NDataModel model = getModel();
        return model == null ? null : model.getAlias();
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return getIndexPlan().listAllTblColRefs();
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        return getIndexPlan().listAllColumnDescs();
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return Lists.newArrayList(getIndexPlan().getEffectiveDimCols().values());
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        Collection<NDataModel.Measure> measures = getIndexPlan().getEffectiveMeasures().values();
        List<MeasureDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        result.addAll(measures);
        return result;
    }

    public FunctionDesc findAggrFuncFromDataflowDesc(FunctionDesc aggrFunc) {
        for (MeasureDesc measure : this.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    @Override
    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.ONLINE;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + getModel().getAlias() + "]";
    }

    @Override
    public long getDateRangeStart() {
        return segments.getTSStart();
    }

    @Override
    public long getDateRangeEnd() {
        return segments.getTSEnd();
    }

    public NDataSegment getSegment(String segId) {
        for (NDataSegment seg : segments) {
            if (seg.getId().equals(segId))
                return seg;
        }
        return null;
    }

    public Segments<NDataSegment> getMergingSegments(NDataSegment mergedSegment) {
        return segments.getMergingSegments(mergedSegment);
    }

    public Segments<NDataSegment> getQueryableSegments() {
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, project);
        val loadingRange = loadingRangeManager.getDataLoadingRange(getModel().getRootFactTableName());
        if (loadingRange == null) {
            return getSegments(SegmentStatusEnum.READY);
        } else {
            val querableRange = loadingRangeManager.getQuerableSegmentRange(loadingRange);
            return segments.getSegments(SegmentStatusEnum.READY).getSegmentsByRange(querableRange);
        }
    }

    public Segments<NDataSegment> getSegments(SegmentStatusEnum status) {
        return segments.getSegments(status);
    }

    public Segments<NDataSegment> getFlatSegments() {
        return segments.getFlatSegments();
    }

    public Segments<NDataSegment> calculateToBeSegments(NDataSegment newSegment) {
        return segments.calculateToBeSegments(newSegment);
    }

    public Segments<NDataSegment> getBuildingSegments() {
        return segments.getBuildingSegments();
    }

    public NDataSegment getFirstSegment() {
        List<NDataSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(0);
        }
    }

    public NDataSegment getLatestReadySegment() {
        Segments<NDataSegment> readySegment = getSegments(SegmentStatusEnum.READY);
        if (readySegment.isEmpty()) {
            return null;
        } else {
            return readySegment.get(readySegment.size() - 1);
        }
    }

    public NDataSegment getLastSegment() {
        List<NDataSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    public SegmentRange getCoveredRange() {
        List<NDataSegment> segs = getFlatSegments();
        if (segs.isEmpty()) {
            return null;
        } else {
            return segs.get(0).getSegRange().coverWith(segs.get(segs.size() - 1).getSegRange());
        }
    }

    public String getSegmentHdfsPath(String segmentId) {
        String hdfsWorkingDir = KapConfig.wrap(config).getReadHdfsWorkingDirectory();
        String path = hdfsWorkingDir + getProject() + "/parquet/" + getUuid() + "/" + segmentId;
        return path;
    }

    public Segments getSegmentsByRange(SegmentRange range) {
        return segments.getSegmentsByRange(range);
    }

    @Override
    public boolean supportsLimitPushDown() {
        return true; // TODO: storage_type defined on cuboid level, which will decide whether to support
    }

    @Override
    public boolean hasPrecalculatedFields() {
        return true;
    }

    @Override
    public int getStorageType() {
        return IStorageAware.ID_NDATA_STORAGE;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public String getProject() {
        return project;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public String[] getMpValues() {
        return isCachedAndShared() ? Arrays.copyOf(mpValues, mpValues.length) : mpValues;
    }

    public void setMpValues(String[] mpValues) {
        checkIsNotCachedAndShared();
        this.mpValues = mpValues;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        checkIsNotCachedAndShared();
        this.createTimeUTC = createTimeUTC;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        checkIsNotCachedAndShared();
        this.status = status;
    }

    public Segments<NDataSegment> getSegments() {
        return isCachedAndShared() ? new Segments(segments) : segments;
    }

    public void setSegments(Segments<NDataSegment> segments) {
        checkIsNotCachedAndShared();

        Collections.sort(segments);
        segments.validate();

        this.segments = segments;
    }

    public String getOwner() {
        return owner;
    }

    void setOwner(String owner) {
        checkIsNotCachedAndShared();
        this.owner = owner;
    }

    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    public void setStorageLocationIdentifier(String storageLocationIdentifier) {
        checkIsNotCachedAndShared();
        this.storageLocationIdentifier = storageLocationIdentifier;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        checkIsNotCachedAndShared();
        this.cost = cost;
    }

    // ============================================================================

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + uuid.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        NDataflow other = (NDataflow) obj;
        if (!uuid.equals(other.uuid))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "NDataflow [" + getModelAlias() + "]";
    }

    public Segments getSegmentsToRemoveByRetention() {
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, getModel().getUuid());
        val retentionRange = segmentConfig.getRetentionRange();
        if (!retentionRange.isRetentionRangeEnabled() || retentionRange.getRetentionRangeNumber() <= 0
                || retentionRange.getRetentionRangeType() == null) {
            return null;
        } else {
            return segments.getSegmentsToRemoveByRetention(retentionRange.getRetentionRangeType(),
                    retentionRange.getRetentionRangeNumber());
        }
    }

    public boolean checkBrokenWithRelatedInfo() {
        val dfBroken = isBroken();
        if (dfBroken) {
            return dfBroken;
        }
        val cubePlanManager = NIndexPlanManager.getInstance(config, project);
        val cubePlan = cubePlanManager.getIndexPlan(uuid);
        val cubeBroken = cubePlan == null || cubePlan.isBroken();
        if (cubeBroken) {
            return cubeBroken;
        }
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(uuid);
        val modelBroken = model == null || model.isBroken();
        return modelBroken;
    }
}
