package io.kyligence.kap.storage.parquet.format;

import io.kyligence.kap.storage.parquet.format.file.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by roger on 6/15/16.
 */
public class ParquetRawRecordReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private Path shardPath;
    private ParquetBundleReader reader = null;

    private K key;
    private V val;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        shardPath = path;

        String bitsetString = conf.get(ParquetFormatConstants.KYLIN_FILTER_BITSET);
        System.out.println("bitsetString size: " + bitsetString.length());
        ImmutableRoaringBitmap bitset = null;
        if (bitsetString != null) {
            bitset = new ImmutableRoaringBitmap(ByteBuffer.wrap(bitsetString.getBytes("ISO-8859-1")));
        }

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setConf(conf).setPath(shardPath).setPageBitset(bitset).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();
        if (data == null) {
            return false;
        }

        key = (K) new Text(((Binary)data.get(0)).getBytes());
        setVal(data);
        return true;
    }

    private void setVal(List<Object> data) {
        int valueBytesLength = 0;
        for (int i = 1; i < data.size(); ++i) {
            valueBytesLength += ((Binary)data.get(i)).getBytes().length;
        }
        byte[] valueBytes = new byte[valueBytesLength];

        int offset = 0;
        for (int i = 1; i < data.size(); ++i) {
            byte[] src = ((Binary)data.get(i)).getBytes();
            System.arraycopy(src, 0, valueBytes, offset, src.length);
            offset += src.length;
        }

        val = (V)new Text(valueBytes);
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
        reader.close();
    }
}
