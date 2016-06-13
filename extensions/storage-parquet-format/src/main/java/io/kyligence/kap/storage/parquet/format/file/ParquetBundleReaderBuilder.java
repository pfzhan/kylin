package io.kyligence.kap.storage.parquet.format.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by roger on 6/13/16.
 */
public class ParquetBundleReaderBuilder {
    private String indexPathSuffix = "index";
    private Configuration conf;
    private Path path;
    private Path indexPath;
    private List<Integer> columns;

    public ParquetBundleReaderBuilder setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ParquetBundleReaderBuilder setPath(Path path) {
        this.path = path;
        return this;
    }

    public ParquetBundleReaderBuilder setIndexPath(Path indexPath) {
        this.indexPath = indexPath;
        return this;
    }

    public ParquetBundleReaderBuilder setColumns(List<Integer> columns) {
        this.columns = columns;
        return this;
    }

    public ParquetBundleReader build() throws IOException {
        if (conf == null) {
            throw new IllegalStateException("Configuration should be set");
        }

        if (path == null) {
            throw new IllegalStateException("Output file path should be set");
        }

        if (indexPath == null) {
            indexPath = new Path(path.toString() + indexPathSuffix);
        }

        if (columns == null) {
            int columnCnt = new ParquetReaderBuilder().setConf(conf)
                    .setPath(path)
                    .setIndexPath(indexPath)
                    .build().getColumnCount();
            columns = new ArrayList<>(columnCnt);
            for (int i = 0; i < columnCnt; ++i) {
                columns.add(i);
            }
        }

        return new ParquetBundleReader(conf, path, indexPath, columns);
    }
}
