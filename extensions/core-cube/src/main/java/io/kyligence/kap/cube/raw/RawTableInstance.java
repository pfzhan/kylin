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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.IKapStorageAware;

/**
 * RawTableInstance is an optional attachment to CubeInstance. Their linkage is the identical 'name' attribute.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableInstance extends RootPersistentEntity implements IRealization {
    public static final String REALIZATION_TYPE = "INVERTED_INDEX";

    private static final Logger logger = LoggerFactory.getLogger(RawTableInstance.class);

    public static final String RAW_TABLE_INSTANCE_RESOURCE_ROOT = "/raw_table_instance";

    public static RawTableInstance create(String name, RawTableDesc desc) {
        RawTableInstance rawInstance = new RawTableInstance();
        rawInstance.setConfig(desc.getConfig());
        rawInstance.setName(name);
        rawInstance.setDescName(desc.getName());
        rawInstance.setCreateTimeUTC(System.currentTimeMillis());
        rawInstance.setSegments(new Segments<RawTableSegment>());
        rawInstance.setStatus(RealizationStatusEnum.DISABLED);
        rawInstance.updateRandomUuid();
        return rawInstance;
    }

    // ============================================================================
    
    @JsonIgnore
    private KylinConfig config;
    @JsonProperty("name")
    private String name;
    @JsonProperty("descriptor")
    private String descName;
    @JsonProperty("shard_num")
    private Integer shardNumber = 0;
    @JsonProperty("status")
    private RealizationStatusEnum status;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("cost")
    private int cost = 50;
    @JsonManagedReference
    @JsonProperty("segments")
    private Segments<RawTableSegment> segments = new Segments<>();
    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    private Set<TblColRef> allColumns;
    private Set<ColumnDesc> allColumnDescs;
    private List<TblColRef> mockupDimensions;
    private List<MeasureDesc> mockupMeasures;

    // for Jackson
    public RawTableInstance() {
    }

    public void init(KylinConfig config) {
        if (null == config)
            throw new IllegalArgumentException("config is null in RawTableInstance Init!");
        this.config = config;
        initAllColumns();
        initDimensions();
        initMeasures();
    }
    
    @Override
    public String resourceName() {
        return name;
    }

    public RawTableDesc getRawTableDesc() {
        return RawTableDescManager.getInstance(config).getRawTableDesc(descName);
    }

    private void initAllColumns() {
        allColumns = new HashSet<>();
        allColumnDescs = new HashSet<>();
        for (TblColRef col : getRawTableDesc().getColumnsInOrder()) {
            allColumns.add(col);
            allColumnDescs.add(col.getColumnDesc());
        }
    }

    private void initDimensions() {
        mockupDimensions = new ArrayList<>();
        TblColRef orderedColumn = getRawTableDesc().getFirstSortbyColumn();
        if (orderedColumn != null)
            mockupDimensions.add(orderedColumn);
    }

    private void initMeasures() {
        mockupMeasures = new ArrayList<>();
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescName(String name) {
        this.descName = name;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public CubeInstance getCubeInstance() {
        CubeInstance cube = CubeManager.getInstance(config).getCube(name);
        if (cube == null)
            throw new IllegalStateException("Cannot find the cube that this raw table attaches to: " + this);
        return cube;
    }

    public String getRootFactTable() {
        return getModel().getRootFactTable().getTableIdentity();
    }

    @Override
    public int getStorageType() {
        // TODO a new storage type
        // OLAPEnumerator.queryStorage() uses this to create a IStorageQuery
        return IKapStorageAware.ID_SHARDED_PARQUET;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        CapabilityResult result = RawTableCapabilityChecker.check(this, digest);
        result.cost = getCost(digest);

        if (!result.capable) {
            result.cost = -1;
        }
        return result;
    }

    public int getCost(SQLDigest digest) {
        CubeInstance cube = CubeManager.getInstance(getConfig()).getCube(name);
        return 10 * cube.getCost(digest);
    }

    @Override
    public String getType() {
        return REALIZATION_TYPE;
    }

    @Override
    public DataModelDesc getModel() {
        RawTableDesc desc = this.getRawTableDesc();
        if (desc != null) {
            return desc.getModel();
        } else {
            return null;
        }
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return allColumns;
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        return allColumnDescs;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return mockupDimensions;
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return mockupMeasures;
    }

    @Override
    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.READY;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + getName() + "]";
    }

    public String getDescName() {
        return descName;
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
        return true;
    }

    public void setSegments(Segments<RawTableSegment> segments) {
        this.segments = segments;
    }

    public Segments<RawTableSegment> getSegments() {
        return segments;
    }

    // validates that all segments have corresponding cube segments
    public void validateSegments() {
        Set<String> validIds = Sets.newHashSet();
        for (CubeSegment seg : getCubeInstance().getSegments()) {
            validIds.add(seg.getUuid());
        }

        Iterator<RawTableSegment> iter = segments.iterator();
        while (iter.hasNext()) {
            RawTableSegment seg = iter.next();
            if (validIds.contains(seg.getUuid()) == false) {
                iter.remove();
                logger.info("RawTableSegment " + seg + " is removed because corresponding cube segment not found");
            }
        }
    }

    public Segments<RawTableSegment> getBuildingSegments() {
        return segments.getBuildingSegments();
    }

    public Segments<RawTableSegment> getMergingSegments(RawTableSegment mergedSegment) {
        return segments.getMergingSegments(mergedSegment);
    }

    public Segments<RawTableSegment> getSegments(SegmentStatusEnum status) {
        return segments.getSegments(status);
    }

    public RawTableSegment getSegmentById(String segmentId) {
        for (RawTableSegment segment : getSegments()) {
            if (Objects.equal(segment.getUuid(), segmentId)) {
                return segment;
            }
        }
        return null;
    }

    public Integer getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(Integer shardNumber) {
        this.shardNumber = shardNumber;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        this.status = status;
    }

    @Override
    public int getCost() {
        CubeInstance cube = CubeManager.getInstance(getConfig()).getCube(name);
        return 10 * cube.getCost();
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    void setConfig(KylinConfig config) {
        this.config = config;
    }

    public KylinConfig getConfig() {
        return config;
    }

    @Override
    public boolean hasPrecalculatedFields() {
        return false;
    }

    public boolean needAutoMerge() {
        if (!this.getRawTableDesc().getModel().getPartitionDesc().isPartitioned())
            return false;

        return this.getRawTableDesc().getAutoMergeTimeRanges() != null
                && this.getRawTableDesc().getAutoMergeTimeRanges().length > 0;
    }

    public SegmentRange autoMergeCubeSegments() throws IOException {
        return segments.autoMergeCubeSegments(needAutoMerge(), getName(), getRawTableDesc().getAutoMergeTimeRanges());
    }

    public Segments calculateToBeSegments(RawTableSegment newSegment) {
        return segments.calculateToBeSegments(newSegment);
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String instanceName) {
        return RAW_TABLE_INSTANCE_RESOURCE_ROOT + "/" + instanceName + MetadataConstants.FILE_SURFIX;
    }

    @Override
    public String toString() {
        return getCanonicalName();
    }

}
