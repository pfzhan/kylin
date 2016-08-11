package io.kyligence.kap.cube.raw;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.metadata.model.IKapStorageAware;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

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
    private List<TblColRef> dimensions;
    private List<MeasureDesc> measures;

    public RawTableInstance(CubeInstance cube) {
        this.cube = cube;
        rawTableDesc = new RawTableDesc(cube);
        
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
        for (ColumnDesc columnDesc: rawTableDesc.getColumns()) {
            allColumns.add(columnDesc.getRef());
        }
    }

    private void initDimensions() {
        dimensions = new ArrayList<>();
        for (ColumnDesc columnDesc: rawTableDesc.getDimensions()) {
            dimensions.add(columnDesc.getRef());
        }
    }

    private void initMeasures() {
        measures = new ArrayList<>();
        Set<ColumnDesc> dimensionsSet = new HashSet<>();
        for (ColumnDesc columnDesc: rawTableDesc.getDimensions()) {
            dimensionsSet.add(columnDesc);
        }

        for (ColumnDesc columnDesc: rawTableDesc.getColumns()) {
            if (!dimensionsSet.contains(columnDesc)) {
                measures.add(transferToMeasureDesc(columnDesc));
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
        return dimensions;
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return measures;
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

}
