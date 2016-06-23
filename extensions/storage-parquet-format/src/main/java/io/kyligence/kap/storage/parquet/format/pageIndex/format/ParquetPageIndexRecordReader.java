package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;
import org.apache.kylin.common.KylinConfig;

/**
 * RecordReader used in ParquetPageIndexInputFormat.
 *
 * @param <K> Path of parquet data file (*.parquet)
 * @param <V> Instance of ParquetPageIndexTable
 */
public class ParquetPageIndexRecordReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private Path shardIndexPath;
    private ParquetPageIndexTable indexTable = null;
    private FSDataInputStream inputStream = null;

    private K key;
    private V val;
    private boolean hasRead = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        KylinConfig.setKylinConfigFromInputStream(IOUtils.toInputStream(""));

        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        shardIndexPath = fileSplit.getPath();
        inputStream = FileSystem.get(conf).open(shardIndexPath);
        indexTable = new ParquetPageIndexTable(inputStream);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (hasRead) {
            key = null;
            val = null;
            return false;
        } else {
            key = (K) getParquetPath(shardIndexPath);
            val = (V) indexTable;
            hasRead = true;
            return true;
        }
    }

    private String getParquetPath(Path indexFilePath) {
        return indexFilePath.toString().replace(".parquet.inv", ".parquet");
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasRead ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        indexTable.close();
    }
}
