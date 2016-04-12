package io.kyligence.kap.cube;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;

/**
 * Created by dongli on 4/6/16.
 */
public class ColumnIndexFactory {
    public static IColumnForwardIndex createLocalForwardIndex(String colName, int maxValue, String localIdxFilename) {
        return new GTColumnForwardIndex(colName, maxValue, localIdxFilename);
    }

    public static IColumnInvertedIndex createLocalInvertedIndex(String colName, int cardinality, String localIdxFilename) {
        return new GTColumnInvertedIndex(colName, cardinality, localIdxFilename);
    }
}
