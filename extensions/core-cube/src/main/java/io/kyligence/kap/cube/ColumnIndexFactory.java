package io.kyligence.kap.cube;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;

/**
 * Created by dongli on 4/6/16.
 */
public class ColumnIndexFactory {
    public static IColumnForwardIndex createLocalForwardIndex(CubeSegment cubeSegment, TblColRef tblColRef, String localIdxFilename) {
        return new GTColumnForwardIndex(cubeSegment, tblColRef, localIdxFilename);
    }

    public static IColumnInvertedIndex createLocalInvertedIndex(CubeSegment cubeSegment, TblColRef tblColRef, String localIdxFilename) {
        return new GTColumnInvertedIndex(cubeSegment, tblColRef, localIdxFilename);
    }
}
