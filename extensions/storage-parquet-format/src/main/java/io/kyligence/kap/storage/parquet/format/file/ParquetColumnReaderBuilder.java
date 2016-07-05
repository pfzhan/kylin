package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ParquetColumnReaderBuilder {
    private String indexPathSuffix = "index";
    private Configuration conf = null;
    private Path path = null;
    private Path indexPath = null;
    private int column = 0;
    private ImmutableRoaringBitmap pageBitset = null;
    private long fileOffset;

    public ParquetColumnReaderBuilder setIndexPathSuffix(String indexPathSuffix) {
        this.indexPathSuffix = indexPathSuffix;
        return this;
    }

    public ParquetColumnReaderBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetColumnReaderBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetColumnReaderBuilder setIndexPath(Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ParquetColumnReaderBuilder setColumn(int column) {
        this.column = column;
        return this;
    }

    public ParquetColumnReaderBuilder setPageBitset(ImmutableRoaringBitmap bitset) {
        this.pageBitset = bitset;
        return this;
    }

    public ParquetColumnReaderBuilder setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
        return this;
    }

    public ParquetColumnReader build() throws IOException {
        if (conf == null) {
            throw new IllegalStateException("Configuration should be set");
        }

        if (path == null) {
            throw new IllegalStateException("Output file path should be set");
        }

        if (indexPath == null) {
            indexPath = new Path(path.toString() + indexPathSuffix);
        }

        return new ParquetColumnReader(new ParquetRawReader(conf, path, indexPath, fileOffset), column, pageBitset);
    }
}
