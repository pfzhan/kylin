package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ParquetIndexWriter {
    private FSDataOutputStream os;

    public ParquetIndexWriter(Configuration config, Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        os = fileSystem.create(path);
    }

    public void addIndex(int group, int column, int page, long offset) throws IOException {
        os.writeInt(group);
        os.writeInt(column);
        os.writeInt(page);
        os.writeLong(offset);
    }

    public void close() throws IOException {
        os.close();
    }

    public static Path getIndexPath(Path path) {
        return new Path(path.toString() + "index");
    }
}
