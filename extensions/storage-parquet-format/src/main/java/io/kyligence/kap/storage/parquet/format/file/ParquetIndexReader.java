package io.kyligence.kap.storage.parquet.format.file;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ParquetIndexReader {
    private FSDataInputStream is;
    private Map<String, Long> indexMap;
    private Map<Integer, List<GroupOffsetPair>> indexListMap;

    public ParquetIndexReader(Configuration config, Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        is = fileSystem.open(path);
        indexListMap = new HashMap<Integer, List<GroupOffsetPair>>();
        indexMap = getIndex(indexListMap);
    }

    public long getOffset(int group, int column, int page) {
        return indexMap.get(getIndexKey(group, column, page));
    }

    public List<GroupOffsetPair> getOffsets(int column) {
        return indexListMap.get(column);
    }

    public void close() throws IOException {
        is.close();
    }

    private Map<String, Long> getIndex(Map<Integer, List<GroupOffsetPair>> indexListMap) throws IOException {
        int group, column, page;
        long offset;

        Map<String, Long> indexMap = new HashMap<String, Long>();

        try {
            while (true) {
                group = is.readInt();
                column = is.readInt();
                page = is.readInt();
                offset = is.readLong();
                indexMap.put(getIndexKey(group, column, page), offset);

                if (!indexListMap.containsKey(column)) {
                    indexListMap.put(column, new ArrayList<GroupOffsetPair>());
                }
                indexListMap.get(column).add(new GroupOffsetPair(group, offset));
            }
        } catch (EOFException ex) {
            //do nothing?
        }
        return indexMap;
    }

    private String getIndexKey(int group, int column, int page) {
        return group + "," + column + "," + page;
    }

    public static Path getIndexPath(Path path) {
        return new Path(path.toString() + "offset");
    }

    public class GroupOffsetPair {
        private int group;
        private long offset;

        public GroupOffsetPair(int group, long offset) {
            this.group = group;
            this.offset = offset;
        }

        public int getGroup() {
            return group;
        }

        public long getOffset() {
            return offset;
        }
    }
}
