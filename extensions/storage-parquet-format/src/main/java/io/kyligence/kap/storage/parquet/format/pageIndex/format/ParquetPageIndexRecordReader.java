package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
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
    public long initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        shardIndexPath = fileSplit.getPath();
        inputStream = FileSystem.get(conf).open(shardIndexPath);
        assert Longs.BYTES == ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE;
        long fileOffset = inputStream.readLong();
        indexTable = new ParquetPageIndexTable(inputStream);
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
