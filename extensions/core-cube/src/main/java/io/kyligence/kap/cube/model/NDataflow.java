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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
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

        df.setConfig((KylinConfigExt) plan.getConfig());
        df.setName(name);
        df.setCubePlanName(plan.getName());
        df.setCreateTimeUTC(System.currentTimeMillis());
        df.setSegments(new Segments<NDataSegment>());
        df.setStatus(RealizationStatusEnum.DISABLED);
        df.updateRandomUuid();

        return df;
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
    private String[] mpValues;
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

    // ================================================================

    void initAfterReload(KylinConfigExt config) {
        setConfig(config);
        for (NDataSegment seg : segments) {
            seg.initAfterReload();
        }
    }

    public String getResourcePath() {
        return concatResourcePath(name);
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
        r.add(getModel().getProjectInstance().getResourcePath());
        r.add(getModel().getResourcePath());
        for (TableRef t : getModel().getAllTables()) {
            r.add(t.getTableDesc().getResourcePath());
        }

        return r;
    }

    public NCubePlan getCubePlan() {
        return NCubePlanManager.getInstance(config).getCubePlan(cubePlanName);
    }

    public static String concatResourcePath(String name) {
        return DATAFLOW_RESOURCE_ROOT + "/" + name + ".json";
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
    public DataModelDesc getModel() {
        NCubePlan cubePlan = this.getCubePlan();
        if (cubePlan != null) {
            return cubePlan.getModel();
        } else {
            return null;
        }
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return getCubePlan().listAllColumns();
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
        return getStatus() == RealizationStatusEnum.READY;
    }

    public String getName() {
        return name;
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

    @Override
    public boolean supportsLimitPushDown() {
        return true; // TODO: storage_type defined on cuboid level, which will decide whether to support
    }

    public KylinConfigExt getConfig() {
        return config;
    }

    @Override
    public boolean hasPrecalculatedFields() {
        return true;
    }

    public String getDescription() {
        return description;
    }

    public String[] getMpValues() {
        return mpValues;
    }

    public String getCubePlanName() {
        return cubePlanName;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public Segments<NDataSegment> getSegments() {
        return segments;
    }

    public Segments<NDataSegment> getSegments(SegmentStatusEnum status) {
        return segments.getSegments(status);
    }

    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    public void setStatus(RealizationStatusEnum status) {
        this.status = status;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public String getOwner() {
        return owner;
    }

    void setOwner(String owner) {
        this.owner = owner;
    }

    public void setSegments(Segments<NDataSegment> segments) {
        this.segments = segments;
    }

    void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setMpValues(String[] mpValues) {
        this.mpValues = mpValues;
    }

    public void setCubePlanName(String cubePlanName) {
        this.cubePlanName = cubePlanName;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public void setStorageLocationIdentifier(String storageLocationIdentifier) {
        this.storageLocationIdentifier = storageLocationIdentifier;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    @Override
    public int getStorageType() {
        return IKapStorageAware.ID_NDATA_STORAGE;
    }

    public Segments calculateToBeSegments(NDataSegment newSegment) {
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

    public NDataSegment getSegment(int segId) {
        for (NDataSegment seg : segments) {
            if (seg.getId() == segId)
                return seg;
        }
        return null;
    }

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
