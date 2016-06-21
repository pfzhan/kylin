package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.GTScanRequest;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexTable;

/**
 * Created by dong on 6/21/16.
 */
public class ParquetPageIndexRecordReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private GTScanRequest scanRequest;
    private Path shardPath;
    private ParquetPageIndexTable indexTable = null;
    private FSDataInputStream inputStream = null;

    private K key;
    private V val;
    private boolean hasRead = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        shardPath = path;

        scanRequest = GTScanRequest.serializer.deserialize(new ByteArray(conf.get(ParquetFormatConstants.KYLIN_SCAN_REQUEST).getBytes()).asBuffer());

        inputStream = FileSystem.get(conf).open(shardPath);
        indexTable = new ParquetPageIndexTable(inputStream);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (hasRead) {
            return false;
        } else {
            key = (K) new Text(shardPath.toString());
            val = (V) new SerializableImmutableRoaringBitmap(indexTable.lookup(scanRequest.getFilterPushDown()));
            hasRead = true;
            return true;
        }
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
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        indexTable.close();
    }
}
