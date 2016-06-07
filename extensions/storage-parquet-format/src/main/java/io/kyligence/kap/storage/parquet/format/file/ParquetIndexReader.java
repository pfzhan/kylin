package io.kyligence.kap.storage.parquet.format.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetIndexReader {
    private FSDataInputStream is;
    private Map<String, Long> indexMap;
    private Map<Integer, List<IndexBundle>> indexListMap;

    public ParquetIndexReader(Configuration config, Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        is = fileSystem.open(path);
        indexListMap = new HashMap<Integer, List<IndexBundle>>();
        indexMap = getIndex(indexListMap);
    }

    public long getOffset(int group, int column, int page) {
        return indexMap.get(getIndexKey(group, column, page));
    }

    public List<IndexBundle> getOffsets(int column) {
        return indexListMap.get(column);
    }

    public void close() throws IOException {
        is.close();
    }

    private Map<String, Long> getIndex(Map<Integer, List<IndexBundle>> indexListMap) throws IOException {
        int group, column, page;
        long index;

        Map<String, Long> indexMap = new HashMap<String, Long>();

        try {
            while (true) {
                group = is.readInt();
                column = is.readInt();
                page = is.readInt();
                index = is.readLong();
                indexMap.put(getIndexKey(group, column, page), index);

                if (!indexListMap.containsKey(column)) {
                    indexListMap.put(column, new ArrayList<IndexBundle>());
                }
                indexListMap.get(column).add(new IndexBundle(group, index));
            }
        }
        catch (EOFException ex) {
        }
        return indexMap;
    }

    private String getIndexKey(int group, int column, int page) {
        return group + "," + column + "," + page;
    }

    public static Path getIndexPath(Path path) {
        return new Path(path.toString() + "index");
    }

    public class IndexBundle {
        private int group;
        private long index;

        public IndexBundle(int group, long index) {
            this.group = group;
            this.index = index;
        }

        public int getGroup() {
            return group;
        }

        public long getIndex() {
            return index;
        }
    }
}
