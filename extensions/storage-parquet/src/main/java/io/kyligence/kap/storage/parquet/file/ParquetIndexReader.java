package io.kyligence.kap.storage.parquet.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ParquetIndexReader {
    private FSDataInputStream is;
    private Map<String, Long> indexMap;

    public ParquetIndexReader(Configuration config, Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        is = fileSystem.open(path);
        indexMap = getIndex();
    }

    public long getOffset(int group, int column, int page) {
        return indexMap.get(getIndexKey(group, column, page));
    }

    public void close() throws IOException {
        is.close();
    }

    private Map<String, Long> getIndex() throws IOException {
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
}
