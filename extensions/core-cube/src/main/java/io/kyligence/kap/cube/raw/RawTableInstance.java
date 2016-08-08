package io.kyligence.kap.cube.raw;

import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
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
    
    private List<TblColRef> allColumns;
    private List<TblColRef> dimensions;
    private List<MeasureDesc> measures;

    public RawTableInstance(CubeInstance cube) {
        this.cube = cube;
        
        init();
    }

    private void init() {
        // load from data model
        // TODO allColumns =
        // TODO dimensions =
        // TODO measures =
    }

    @Override
    public int getStorageType() {
        // TODO a new storage type
        // OLAPEnumerator.queryStorage() uses this to create a IStorageQuery
        return 0;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        // TODO mimic CubeCapabilityChecker
        return null;
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
