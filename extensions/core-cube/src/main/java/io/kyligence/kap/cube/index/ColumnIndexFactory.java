package io.kyligence.kap.cube.index;

/**
 * Created by dongli on 4/6/16.
 */
public class ColumnIndexFactory {
    public static IColumnForwardIndex createLocalForwardIndex(String colName, int maxValue, String localIdxFilename) {
        return new GTColumnForwardIndex(colName, maxValue, localIdxFilename);
    }

    public static IColumnInvertedIndex createLocalInvertedIndex(String colName, int cardinality, String localIdxFilename) {
        return new GTColumnInvertedIndex(localIdxFilename, colName, cardinality);
    }

    public static IColumnInvertedIndex createLocalInvertedIndexForReader(String localIdxFilename) {
        return new GTColumnInvertedIndex(localIdxFilename);
    }

}
