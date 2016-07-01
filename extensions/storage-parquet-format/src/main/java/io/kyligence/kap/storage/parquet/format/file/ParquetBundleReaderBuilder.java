package io.kyligence.kap.storage.parquet.format.file;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Created by roger on 6/13/16.
 */
public class ParquetBundleReaderBuilder {
    private String indexPathSuffix = "index";
    private Configuration conf;
    private Path path;
    private Path indexPath;
    private ImmutableRoaringBitmap columnBitset = null;
    private ImmutableRoaringBitmap pageBitset = null;
    private long fileOffset = 0;

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

    public ParquetBundleReaderBuilder setColumnsBitmap(ImmutableRoaringBitmap columns) {
        this.columnBitset = columns;
        return this;
    }

    public ParquetBundleReaderBuilder setPageBitset(ImmutableRoaringBitmap bitset) {
        this.pageBitset = bitset;
        return this;
    }

    public ParquetBundleReaderBuilder setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
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

        if (columnBitset == null) {
            int columnCnt = new ParquetRawReaderBuilder().setConf(conf).setPath(path).setIndexPath(indexPath).build().getColumnCount();
            columnBitset = createBitset(columnCnt);
        }

        return new ParquetBundleReader(conf, path, indexPath, columnBitset, pageBitset, fileOffset);
    }

    private static ImmutableRoaringBitmap createBitset(int total) throws IOException {
        MutableRoaringBitmap mBitmap = new MutableRoaringBitmap();
        for (int i = 0; i < total; ++i) {
            mBitmap.add(i);
        }

        ImmutableRoaringBitmap iBitmap = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos);) {
            mBitmap.serialize(dos);
            dos.flush();
            iBitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(baos.toByteArray()));
        }

        return iBitmap;
    }
}
