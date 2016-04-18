package io.kyligence.kap.cube.gridtable;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.TblColRef;

public class GTUtilExd {

    static public TblColRef getRealColFromMockUp(TblColRef mockUpCol, Cuboid cuboid) {
        return cuboid.getColumns().get(mockUpCol.getColumnDesc().getZeroBasedIndex());
    }
}
