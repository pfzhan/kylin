package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ParquetTarballReader extends ParquetRawReader {
    private long skipLength = 0;

    public ParquetTarballReader(Configuration config, Path path, Path indexPath) throws IOException {
        super(config, path, indexPath, 0);
        skipLength = getSkipOffset(config, path);
    }

    public GeneralValuesReader getValuesReader(int globalPageIndex, int column) throws IOException {
        int group = globalPageIndex / pagesPerGroup;
        int page = globalPageIndex % pagesPerGroup;
        if (!indexMap.containsKey(group + "," + column + "," + page)) {
            return null;
        }
        long offset = Long.parseLong(indexMap.get(group + "," + column + "," + page));
        return getValuesReaderFromOffset(group, column, offset + skipLength);
    }

    public GeneralValuesReader getValuesReader(int rowGroup, int column, int pageIndex) throws IOException {
        long pageOffset = Long.parseLong(indexMap.get(rowGroup + "," + column + "," + pageIndex));
        return getValuesReaderFromOffset(rowGroup, column, pageOffset + skipLength);
    }

    private long getSkipOffset(Configuration config, Path path) throws IOException {
        FileSystem fs = FileSystem.get(config);
        FSDataInputStream in = fs.open(path);
        skipLength = in.readLong();
        return skipLength;
    }
}
