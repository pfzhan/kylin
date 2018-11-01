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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
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
import io.kyligence.kap.metadata.model.IKapStorageAware;
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataflow extends RootPersistentEntity implements IRealization, IKeep, IKapStorageAware {
    public static final String REALIZATION_TYPE = "NCUBE";
    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";

    public static NDataflow create(String name, NCubePlan plan) {
        NDataflow df = new NDataflow();

        df.config = (KylinConfigExt) plan.getConfig();
        df.setName(name);
        df.setProject(plan.getProject());
        df.setCubePlanName(plan.getName());
        df.setCreateTimeUTC(System.currentTimeMillis());
        df.setSegments(new Segments<NDataSegment>());
        df.setStatus(RealizationStatusEnum.NEW);
        df.updateRandomUuid();

        return df;
    }

    public static String concatResourcePath(String name) {
        return DATAFLOW_RESOURCE_ROOT + "/" + name + ".json";
    }

    // ============================================================================

    @JsonIgnore
    private KylinConfigExt config;
    @JsonProperty("name")
    private String name;
    @JsonProperty("description")
    private String description;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("mp_values")
    private String[] mpValues = StringUtil.EMPTY_ARRAY;
    @JsonProperty("cube_plan")
    private String cubePlanName;
    @JsonProperty("create_time_utc")
    private long createTimeUTC;
    @JsonProperty("status")
    private RealizationStatusEnum status;
    @JsonProperty("cost")
    private int cost = 50;

    @JsonManagedReference
    @JsonProperty("segments")
    private Segments<NDataSegment> segments = new Segments<NDataSegment>();

    @JsonProperty("storage_location_identifier")
    private String storageLocationIdentifier; // maybe useful in some cases..

    private String project;

    // ================================================================

    void initAfterReload(KylinConfigExt config) {
        this.config = config;
        for (NDataSegment seg : segments) {
            seg.initAfterReload();
        }
    }

    public KylinConfigExt getConfig() {
        return config;
    }

    public NDataflow copy() {
        return NDataflowManager.getInstance(config, project).copy(this);
    }

    @Override
    public String resourceName() {
        return name;
    }

    public String getResourcePath() {
        return "/" + project + DATAFLOW_RESOURCE_ROOT + "/" + name + MetadataConstants.FILE_SURFIX;
    }

    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // dataflow & segments
        r.add(this.getResourcePath());
        for (NDataSegment seg : segments) {
            r.add(seg.getSegDetails().getResourcePath());
        }

        // cubing plan
        r.add(getCubePlan().getResourcePath());

        // project & model & tables
        r.add(getModel().getProjectInstance().getProjectResourcePath());
        r.add(getModel().getResourcePath());
        for (TableRef t : getModel().getAllTables()) {
            r.add(t.getTableDesc().getResourcePath());
        }

        return r;
    }

    public NCubePlan getCubePlan() {
        return NCubePlanManager.getInstance(config, project).getCubePlan(cubePlanName);
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
        NCubePlan cubePlan = this.getCubePlan();
        if (cubePlan != null) {
            return cubePlan.getModel();
        } else {
            return null;
        }
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return getCubePlan().listAllTblColRefs();
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        return getCubePlan().listAllColumnDescs();
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return Lists.newArrayList(getCubePlan().getEffectiveDimCols().values());
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        Collection<NDataModel.Measure> measures = getCubePlan().getEffectiveMeasures().values();
        List<MeasureDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        result.addAll(measures);
        return result;
    }

    @Override
    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.ONLINE;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + name + "]";
    }

    @Override
    public long getDateRangeStart() {
        return segments.getTSStart();
    }

    @Override
    public long getDateRangeEnd() {
        return segments.getTSEnd();
    }

    public NDataSegment getSegment(int segId) {
        for (NDataSegment seg : segments) {
            if (seg.getId() == segId)
                return seg;
        }
        return null;
    }

    public Segments<NDataSegment> getMergingSegments(NDataSegment mergedSegment) {
        return segments.getMergingSegments(mergedSegment);
    }

    public Segments<NDataSegment> getSegments(SegmentStatusEnum status) {
        return segments.getSegments(status);
    }

    public Segments<NDataSegment> calculateToBeSegments(NDataSegment newSegment) {
        return segments.calculateToBeSegments(newSegment);
    }

    public Segments<NDataSegment> getBuildingSegments() {
        return segments.getBuildingSegments();
    }

    public NDataSegment getLastSegment() {
        List<NDataSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    //get the segment with max id
    public NDataSegment getSegmentWithMaxId() {
        List<NDataSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            int maxIndex = 0;
            for (int i = 1; i < existing.size(); i++) {
                if (existing.get(maxIndex).getId() < existing.get(i).getId())
                    maxIndex = i;
            }
            return existing.get(maxIndex);
        }
    }

    public boolean checkAllowedOnline() {
        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getConfig(),
                getProject());
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager
                .getDataLoadingRange(getModel().getRootFactTableName());
        if (dataLoadingRange == null) {
            return true;
        } else {
            Segments readySegments = this.getSegments(SegmentStatusEnum.READY);
            if (CollectionUtils.isEmpty(readySegments)) {
                return false;
            }
            SegmentRange readyRange = readySegments.getFirstSegment().getSegRange()
                    .coverWith(readySegments.getLatestReadySegment().getSegRange());
            if (dataLoadingRange.getActualQueryStart() == -1 && dataLoadingRange.getActualQueryEnd() == -1) {
                return true;
            }
            if (Long.parseLong(readyRange.getStart().toString()) <= dataLoadingRange.getActualQueryStart()
                    && Long.parseLong(readyRange.getEnd().toString()) >= dataLoadingRange.getActualQueryEnd()) {
                return true;
            } else {
                return false;
            }
        }
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
    public LookupStringTable getLookupTable(String lookupTableName) {
        //lookupTableName is assumed to be in this model for sure

        TableRef table = getModel().findTable(lookupTableName);
        JoinDesc joinByPKSide = getModel().getJoinByPKSide(table);
        return NDataflowManager.getInstance(getConfig(), getProject()).getLookupTable(this.getLastSegment(),
                joinByPKSide);
    }

    @Override
    public int getStorageType() {
        return IKapStorageAware.ID_NDATA_STORAGE;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public String getName() {
        return name;
    }

    void setName(String name) {
        checkIsNotCachedAndShared();
        this.name = name;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
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

    public String getCubePlanName() {
        return cubePlanName;
    }

    public void setCubePlanName(String cubePlanName) {
        checkIsNotCachedAndShared();
        this.cubePlanName = cubePlanName;
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
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "NDataflow [" + name + "]";
    }

}
