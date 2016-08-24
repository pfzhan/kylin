package io.kyligence.kap.cube.model;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DataModelFlatTableDesc implements IJoinedFlatTableDesc {

    private String tableName;
    private final CubeDesc cubeDesc;
    private final DataModelDesc dataModelDesc;
    private final CubeSegment cubeSegment;

    private int columnCount;

    private List<TblColRef> columnList = Lists.newArrayList();
    private Map<TblColRef, Integer> columnIndexMap;

    public DataModelFlatTableDesc(CubeSegment cubeSegment) {
        this(cubeSegment.getCubeDesc(), cubeSegment);
    }

    private DataModelFlatTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment /* can be null */) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.columnIndexMap = Maps.newHashMap();
        this.dataModelDesc = cubeDesc.getModel();
        parseCubeDesc();
    }

    // check what columns from hive tables are required, and index them
    private void parseCubeDesc() {
        if (cubeSegment == null) {
            this.tableName = "kylin_intermediate_" + dataModelDesc.getName();
        } else {
            this.tableName = "kylin_intermediate_" + dataModelDesc.getName() + "_" + cubeSegment.getUuid().replaceAll("-", "_");
        }

        // add dimensions
        int columnIndex = 0;
        for (ModelDimensionDesc mdDesc : dataModelDesc.getDimensions()) {
            for (String col : mdDesc.getColumns()) {
                TblColRef tblColRef = getTblByName(col, mdDesc.getTable());
                columnIndexMap.put(tblColRef, columnIndex);
                columnList.add(tblColRef);
                columnIndex++;
            }
        }

        // add metrics
        for (String metric : dataModelDesc.getMetrics()) {
            TblColRef tblColRef = getTblByName(metric, dataModelDesc.getFactTable());
            if (!columnIndexMap.containsKey(tblColRef)) {
                columnIndexMap.put(tblColRef, columnIndex);
                columnList.add(tblColRef);
                columnIndex++;
            }
        }

        int lookupLength = dataModelDesc.getLookups().length;
        for (int i = 0; i < lookupLength; i++) {
            JoinDesc join = dataModelDesc.getLookups()[i].getJoin();
            for (TblColRef primary: join.getPrimaryKeyColumns()) {
                if (!columnIndexMap.containsKey(primary)) {
                    columnIndexMap.put(primary, columnIndex);
                    columnList.add(primary);
                    columnIndex++;
                }
            }

            for (TblColRef foreign: join.getForeignKeyColumns()) {
                if (!columnIndexMap.containsKey(foreign)) {
                    columnIndexMap.put(foreign, columnIndex);
                    columnList.add(foreign);
                    columnIndex++;
                }
            }
        }
        columnCount = columnIndex;
    }

    private TblColRef getTblByName(String colName, String tableName) {
        if (isFactTable(tableName)) {
            for (ColumnDesc col : dataModelDesc.getFactTableDesc().getColumns()) {
                if (col.getName().equals(colName))
                    return col.getRef();
            }
        } else {
            for (TableDesc desc : dataModelDesc.getLookupTableDescs()) {
                if (!(desc.getDatabase() + "." + desc.getName()).equals(tableName))
                    continue;

                for (ColumnDesc col : desc.getColumns()) {
                    if (col.getName().equals(colName))
                        return col.getRef();
                }
            }
        }
        return null;
    }

    private boolean isFactTable(String tableName) {
        return dataModelDesc.getFactTable().equals(tableName);
    }

    // sanity check the input record (in bytes) matches what's expected
    public void sanityCheck(BytesSplitter bytesSplitter) {
        if (columnCount != bytesSplitter.getBufferSize()) {
            throw new IllegalArgumentException("Expect " + columnCount + " columns, but see " + bytesSplitter.getBufferSize() + " -- " + bytesSplitter);
        }

        // TODO: check data types here
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columnList;
    }

    @Override
    public DataModelDesc getDataModel() {
        return this.dataModelDesc;
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            throw new IllegalArgumentException("Column " + colRef.toString() + " wasn't found on flat table.");

        return index.intValue();
    }

    @Override
    public long getSourceOffsetStart() {
        return cubeSegment.getSourceOffsetStart();
    }

    @Override
    public long getSourceOffsetEnd() {
        return cubeSegment.getSourceOffsetEnd();
    }

    @Override
    public TblColRef getDistributedBy() {
        return cubeDesc.getDistributedByColumn();
    }
}
