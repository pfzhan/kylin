package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ParquetRawReaderBuilder {
    private String indexPathSuffix = "index";
    private Configuration conf = null;
    private Path path = null;
    private Path indexPath = null;

    public ParquetRawReaderBuilder setIndexPathSuffix(String indexPathSuffix) {
        this.indexPathSuffix = indexPathSuffix;
        return this;
    }

    public ParquetRawReaderBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetRawReaderBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetRawReaderBuilder setIndexPath(Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ParquetRawReader build() throws IOException {
        if (conf == null) {
            throw new IllegalStateException("Configuration should be set");
        }

        if (path == null) {
            throw new IllegalStateException("Output file path should be set");
        }

        if (indexPath == null) {
            indexPath = new Path(path.toString() + indexPathSuffix);
        }

        return new ParquetRawReader(conf, path, indexPath);
    }
}
