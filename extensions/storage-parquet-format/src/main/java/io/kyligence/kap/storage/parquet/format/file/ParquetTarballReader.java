package io.kyligence.kap.storage.parquet.format.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class ParquetTarballReader extends ParquetRawReader{
    private long skipLength = 0;
    public ParquetTarballReader (Configuration config, Path path, Path indexPath) throws IOException {
        super(config, path, indexPath);
        skipLength = getSkipOffset(config, path);
    }

    public GeneralValuesReader getValuesReader(int globalPageIndex, int column) throws IOException {
        List<ParquetIndexReader.GroupOffsetPair> indexBundleList = indexReader.getOffsets(column);
        if (globalPageIndex >= indexBundleList.size()) {
            return null;
        }
        ParquetIndexReader.GroupOffsetPair indexBundle = indexBundleList.get(globalPageIndex);
        return getValuesReaderFromOffset(indexBundle.getGroup(), column, indexBundle.getOffset() + skipLength);
    }

    public GeneralValuesReader getValuesReader(int rowGroup, int column, int pageIndex) throws IOException {
        long pageOffset = indexReader.getOffset(rowGroup, column, pageIndex);
        return getValuesReaderFromOffset(rowGroup, column, pageOffset + skipLength);
    }

    private long getSkipOffset(Configuration config, Path path) throws IOException {
        FileSystem fs = FileSystem.get(config);
        FSDataInputStream in = fs.open(path);
        skipLength = in.readLong();
        return skipLength;
    }
}
