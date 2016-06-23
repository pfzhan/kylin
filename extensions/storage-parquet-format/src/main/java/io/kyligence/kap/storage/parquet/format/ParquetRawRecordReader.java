package io.kyligence.kap.storage.parquet.format;

import io.kyligence.kap.storage.parquet.format.file.*;
import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;

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

        String bitsetString = conf.get(ParquetFormatConstants.KYLIN_FILTER_BITSET_MAP);
        System.out.println("bitsetString size: " + bitsetString.length());
        ImmutableRoaringBitmap bitmap = null;

        if (bitsetString != null) {
            ByteArrayInputStream bais = new ByteArrayInputStream(bitsetString.getBytes("ISO-8859-1"));
            HashMap<String, SerializableImmutableRoaringBitmap> bitsetMap = null;
            try {
                bitsetMap = (HashMap<String, SerializableImmutableRoaringBitmap>)(new ObjectInputStream(bais).readObject());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            if (bitsetMap != null) {
                System.out.println("path.toString: " + path.toString());
                bitmap = bitsetMap.get(path.toString()).getBitmap();
            }
        }

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setConf(conf).setPath(shardPath).setPageBitset(bitmap).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();
        if (data == null) {
            return false;
        }

        key = (K) new Text();
        setVal(data);
        return true;
    }

    // We put all columns in values and keep key empty
    private void setVal(List<Object> data) {
        int valueBytesLength = 0;
        for (int i = 0; i < data.size(); ++i) {
            valueBytesLength += ((Binary)data.get(i)).getBytes().length;
        }
        byte[] valueBytes = new byte[valueBytesLength];

        int offset = 0;
        for (int i = 0; i < data.size(); ++i) {
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
