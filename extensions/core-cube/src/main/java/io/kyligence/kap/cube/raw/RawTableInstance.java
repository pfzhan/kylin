package io.kyligence.kap.cube.raw;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.gridtable.RawToGridTableMapping;
import io.kyligence.kap.metadata.model.IKapStorageAware;

/**
 * RawTable is a parasite on Cube (at the moment).
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableInstance extends RootPersistentEntity implements IRealization {

    public static final String RAW_TABLE_INSTANCE_RESOURCE_ROOT = "/raw_table_instance";

    public static boolean isRawTableEnabled(CubeDesc cube) {
        return cube.getOverrideKylinProps().containsKey("kylin.rawtable.enabled");
    }

    public static void setRawTableEnabled(CubeDesc cube) {
        cube.getOverrideKylinProps().put("kylin.rawtable.enabled", "true");
    }

    // ============================================================================

    private CubeInstance cube;

    @JsonProperty("name")
    private String name;
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("rawtable_desc")
    private RawTableDesc rawTableDesc;
    @JsonProperty("shard_num")
    private Integer shardNumber;

    private RawToGridTableMapping mapping = null;
    private List<TblColRef> allColumns;
    private List<TblColRef> mockupDimensions;
    private List<MeasureDesc> mockupMeasures;

    // for Jackson
    public RawTableInstance() {
    }

    // FIXME: this constructor should be package private
    public RawTableInstance(CubeInstance cube) {

        this.name = cube.getName();
        this.uuid = cube.getUuid();
        this.cube = cube;
        this.rawTableDesc = RawTableDescManager.getInstance(cube.getConfig()).getRawTableDesc(cube.getName());

        if (this.rawTableDesc == null) // mockup for test
            this.rawTableDesc = new RawTableDesc(cube.getDescriptor());

        initAllColumns();
        initDimensions();
        initMeasures();
    }

    public void init(KylinConfig config) {
        rawTableDesc.init(config);
        initAllColumns();
        initDimensions();
        initMeasures();
    }

    public RawTableDesc getRawTableDesc() {
        return rawTableDesc;
    }

    //TODO: use its own uuid
    public String getUuid() {
        return uuid;
    }

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

    // Put column type to measure descriptor's first parameter
    private MeasureDesc transferToMeasureDesc(ColumnDesc columnDesc) {
        MeasureDesc measureDesc = new MeasureDesc();
        FunctionDesc functionDesc = new FunctionDesc();
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType(columnDesc.getType().toString());
        functionDesc.setParameter(parameterDesc);
        measureDesc.setFunction(functionDesc);

        measureDesc.setName(columnDesc.getName());

        return measureDesc;
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
        result.cost = 10 * this.cube.getCost(digest);//cube precedes raw in normal cases

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
        return cube.getDataModelDesc();
    }

    @Override
    public String getFactTable() {
        return cube.getFactTable();
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
        return cube.isReady();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + cube.getName() + "]";
    }

    @Override
    public long getDateRangeStart() {
        return cube.getDateRangeStart();
    }

    @Override
    public long getDateRangeEnd() {
        return cube.getDateRangeEnd();
    }

    @Override
    public boolean supportsLimitPushDown() {
        return true;
    }

    public List<RawTableSegment> getSegments() {
        return asRawTableSegments(cube.getSegments());
    }

    public List<RawTableSegment> getSegments(SegmentStatusEnum status) {
        return asRawTableSegments(cube.getSegments(status));
    }

    public Integer getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(Integer shardNumber) {
        this.shardNumber = shardNumber;
    }

    private List<RawTableSegment> asRawTableSegments(List<CubeSegment> cubeSegs) {
        List<RawTableSegment> segs = Lists.newArrayList();
        for (CubeSegment seg : cubeSegs) {
            segs.add(new RawTableSegment(this, seg));
        }
        return segs;
    }

    @Override
    public int hashCode() {
        return cube.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RawTableInstance other = (RawTableInstance) obj;
        if (cube == null) {
            if (other.cube != null) {
                return false;
            }
        } else if (!cube.equals(other.cube)) {
            return false;
        }

        return true;
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
}
