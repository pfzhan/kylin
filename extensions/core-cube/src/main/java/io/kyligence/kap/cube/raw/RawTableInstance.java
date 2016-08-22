package io.kyligence.kap.cube.raw;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.IKapStorageAware;

/**
 * RawTable is a parasite on Cube (at the moment).
 */
public class RawTableInstance implements IRealization {

    public static boolean isRawTableEnabled(CubeDesc cube) {
        return cube.getOverrideKylinProps().containsKey("kylin.rawtable.enabled");
    }

    public static void setRawTableEnabled(CubeDesc cube) {
        cube.getOverrideKylinProps().put("kylin.rawtable.enabled", "true");
    }

    // ============================================================================

    private CubeInstance cube;
    private RawTableDesc rawTableDesc;

    private List<TblColRef> allColumns;
    private List<TblColRef> mockupDimensions;
    private List<MeasureDesc> mockupMeasures;
    
    public RawTableInstance(CubeInstance cube) {
        this.cube = cube;
        this.rawTableDesc = RawTableDescManager.getInstance(cube.getConfig()).getRawTableDesc(cube.getName());

        if (this.rawTableDesc == null) // mockup for test
            this.rawTableDesc = new RawTableDesc(cube.getDescriptor());

        init();
    }

    private void init() {
        // load from data model
        initAllColumns();
        initDimensions();
        initMeasures();
    }

    public RawTableDesc getRawTableDesc() {
        return rawTableDesc;
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
        for (TblColRef col : rawTableDesc.getColumns()) {
            if (!mockupDimensions.contains(col)) {
                mockupMeasures.add(transferToMeasureDesc(col.getColumnDesc()));
            }
        }
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
        return IKapStorageAware.ID_RAWTABLE_SHARDED_PARQUET;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        // TODO mimic CubeCapabilityChecker
        CapabilityResult result = new CapabilityResult();
        result.capable = true;
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
        return cube.getName();
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

    public List<RawTableSegment> getSegments() {
        return asRawTableSegments(cube.getSegments());
    }

    public List<RawTableSegment> getSegments(SegmentStatusEnum status) {
        return asRawTableSegments(cube.getSegments(status));
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
}
