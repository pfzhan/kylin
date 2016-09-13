package io.kyligence.kap.cube.raw;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.raw.gridtable.RawToGridTableMapping;
import io.kyligence.kap.metadata.model.IKapStorageAware;

/**
 * RawTableInstance is an optional attachment to CubeInstance. Their linkage is the identical 'name' attribute.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableInstance extends RootPersistentEntity implements IRealization {

    private static final Logger logger = LoggerFactory.getLogger(RawTableInstance.class);

    public static final String RAW_TABLE_INSTANCE_RESOURCE_ROOT = "/raw_table_instance";

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
    private List<RawTableSegment> segments = new ArrayList<RawTableSegment>();
    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    private RawTableDesc rawTableDesc;
    private RawToGridTableMapping mapping;
    private List<TblColRef> allColumns;
    private List<TblColRef> mockupDimensions;
    private List<MeasureDesc> mockupMeasures;

    // for Jackson
    public RawTableInstance() {
    }

    public static RawTableInstance create(String name, RawTableDesc desc) {
        RawTableInstance rawInstance = new RawTableInstance();
        rawInstance.setConfig(desc.getConfig());
        rawInstance.setName(name);
        rawInstance.setDescName(desc.getName());
        rawInstance.setCreateTimeUTC(System.currentTimeMillis());
        rawInstance.setSegments(new ArrayList<RawTableSegment>());
        rawInstance.setStatus(RealizationStatusEnum.DISABLED);
        rawInstance.updateRandomUuid();
        return rawInstance;
    }

    public void init(KylinConfig config) {
        if (null == config)
            throw new IllegalArgumentException("config is null in RawTableInstance Init!");
        this.rawTableDesc = RawTableDescManager.getInstance(config).getRawTableDesc(descName);
        this.config = config;
        initAllColumns();
        initDimensions();
        initMeasures();
    }

    public RawTableDesc getRawTableDesc() {
        return this.rawTableDesc;
    }

    //TODO: should get from descName
    private void initAllColumns() {
        allColumns = new ArrayList<>();
        for (TblColRef col : rawTableDesc.getColumns()) {
            allColumns.add(col);
        }
    }

    private void initDimensions() {
        mockupDimensions = new ArrayList<>();
        TblColRef orderedColumn = rawTableDesc.getOrderedColumn();
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

    @Override
    public int getStorageType() {
        // TODO a new storage type
        // OLAPEnumerator.queryStorage() uses this to create a IStorageQuery
        return IKapStorageAware.ID_SHARDED_PARQUET;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        CapabilityResult result = RawTableCapabilityChecker.check(this, digest);
        CubeInstance cube = CubeManager.getInstance(getConfig()).getCube(name);
        result.cost = 10 * cube.getCost(digest);//cube precedes raw in normal cases

        if (!result.capable) {
            result.cost = -1;
        }
        return result;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.INVERTED_INDEX;
    }

    @Override
    public DataModelDesc getDataModelDesc() {
        RawTableDesc desc = this.getRawTableDesc();
        if (desc != null) {
            return desc.getModel();
        } else {
            return null;
        }
    }

    @Override
    public String getFactTable() {
        return getRawTableDesc().getModel().getFactTable();
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return allColumns;
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

    @Override
    public long getDateRangeStart() {
        List<RawTableSegment> readySegs = getSegments(SegmentStatusEnum.READY);

        long startTime = Long.MAX_VALUE;
        for (RawTableSegment seg : readySegs) {
            startTime = Math.min(startTime, seg.getDateRangeStart());
        }

        return startTime;
    }

    @Override
    public long getDateRangeEnd() {
        List<RawTableSegment> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = Long.MIN_VALUE;
        for (RawTableSegment seg : readySegs) {
            endTime = Math.max(endTime, seg.getDateRangeEnd());
        }

        return endTime;
    }

    @Override
    public boolean supportsLimitPushDown() {
        return true;
    }

    public void setSegments(List<RawTableSegment> segments) {
        this.segments = segments;
    }

    public List<RawTableSegment> getSegments() {
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

    public List<RawTableSegment> getBuildingSegments() {
        List<RawTableSegment> buildingSegments = new ArrayList<RawTableSegment>();
        if (null != segments) {
            for (RawTableSegment segment : getSegments()) {
                if (SegmentStatusEnum.NEW == segment.getStatus() || SegmentStatusEnum.READY_PENDING == segment.getStatus()) {
                    buildingSegments.add(segment);
                }
            }
        }
        return buildingSegments;
    }

    public List<RawTableSegment> getMergingSegments(RawTableSegment mergedSegment) {
        LinkedList<RawTableSegment> result = new LinkedList<RawTableSegment>();
        if (mergedSegment == null)
            return result;

        for (RawTableSegment seg : getSegments()) {
            if (seg.getStatus() != SegmentStatusEnum.READY && seg.getStatus() != SegmentStatusEnum.READY_PENDING)
                continue;

            if (seg == mergedSegment)
                continue;

            if (mergedSegment.sourceOffsetContains(seg)) {
                // make sure no holes
                if (result.size() > 0 && result.getLast().getSourceOffsetEnd() != seg.getSourceOffsetStart())
                    throw new IllegalStateException("Merging segments must not have holes between " + result.getLast() + " and " + seg);

                result.add(seg);
            }
        }
        return result;
    }

    public List<RawTableSegment> getSegments(SegmentStatusEnum status) {
        List<RawTableSegment> result = new ArrayList<RawTableSegment>();

        for (RawTableSegment segment : getSegments()) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }
        return result;
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

    public int getCost() {
        return cost;
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

    public boolean needAutoMerge() {
        if (!this.getRawTableDesc().getModel().getPartitionDesc().isPartitioned())
            return false;

        return this.getRawTableDesc().getAutoMergeTimeRanges() != null && this.getRawTableDesc().getAutoMergeTimeRanges().length > 0;
    }

    public RawToGridTableMapping getRawToGridTableMapping() {
        if (mapping == null) {
            mapping = new RawToGridTableMapping(this);
        }
        return mapping;
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
