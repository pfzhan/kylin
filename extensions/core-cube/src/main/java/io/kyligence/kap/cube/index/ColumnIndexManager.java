package io.kyligence.kap.cube.index;

/**
 * Created by dongli on 6/1/16.
 */
public class ColumnIndexManager {
    private static ColumnIndexManager INSTANCE = new ColumnIndexManager();

    public static ColumnIndexManager getInstance() {
        return INSTANCE;
    }

    private ColumnIndexManager() {
    }

    public IColumnInvertedIndex getColumnInvertedIndexFromFile(String filename) {
        return ColumnIndexFactory.createLocalInvertedIndexForReader(filename);
    }
}
