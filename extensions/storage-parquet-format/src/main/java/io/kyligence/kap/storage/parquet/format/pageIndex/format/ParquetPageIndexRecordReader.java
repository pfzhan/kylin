package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.primitives.Longs;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetOrderedPageIndexTable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;

/**
 * RecordReader used in ParquetPageIndexInputFormat.
 *
 */
public class ParquetPageIndexRecordReader {
    protected Configuration conf;

    private Path shardIndexPath;
    private ParquetPageIndexTable indexTable = null;
    private FSDataInputStream inputStream = null;

    /**
     * @return file offset to the actual cube data
     */
    public long initialize(Path indexPath, TaskAttemptContext context, boolean hasOffset, boolean ordered) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        shardIndexPath = indexPath;
        inputStream = FileSystem.get(conf).open(shardIndexPath);
        assert Longs.BYTES == ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE;
        long fileOffset = 0;

        if (hasOffset) {
            fileOffset = inputStream.readLong();
        }

        int indexOffset = hasOffset ? ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE : 0;

        if (ordered) {
            indexTable = new ParquetOrderedPageIndexTable(inputStream, indexOffset);
        } else {
            indexTable = new ParquetPageIndexTable(inputStream, indexOffset);
        }
        return fileOffset;
    }

    public ParquetPageIndexTable getIndexTable() {
        return indexTable;
    }

    public void close() throws IOException {
        inputStream.close();
        indexTable.close();
    }
}
