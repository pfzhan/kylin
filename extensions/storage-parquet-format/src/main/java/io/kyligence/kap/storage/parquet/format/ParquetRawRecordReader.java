package io.kyligence.kap.storage.parquet.format;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;
import io.kyligence.kap.storage.parquet.format.serialize.SerializableImmutableRoaringBitmap;

public class ParquetRawRecordReader extends RecordReader<byte[], byte[]> {
    protected Configuration conf;

    private Path shardPath;
    private ParquetBundleReader reader = null;

    private static byte[] key = new byte[0];
    private byte[] val = null;//reusing the val bytes, the returned bytes might contain useless tail

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        shardPath = path;

        ImmutableRoaringBitmap pageBitmap = readBitmapSet(path, ParquetFormatConstants.KYLIN_FILTER_PAGE_BITSET_MAP);
        ImmutableRoaringBitmap measureBitmap = readBitmap(ParquetFormatConstants.KYLIN_FILTER_MEASURES_BITSET_MAP);
        MutableRoaringBitmap columnBitmap = MutableRoaringBitmap.bitmapOf(0);
        for (int i : measureBitmap) {
            columnBitmap.add(i + 1);
        }
        System.out.println("All columns read by parquet: " + StringUtils.join(columnBitmap, ","));
        int gtMaxLength = Integer.valueOf(conf.get(ParquetFormatConstants.KYLIN_GT_MAX_LENGTH));
        val = new byte[gtMaxLength];

        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setConf(conf).setPath(shardPath).setPageBitset(pageBitmap).setColumnsBitmap(columnBitmap).build();
    }

    private ImmutableRoaringBitmap readBitmap(String property) throws IOException {
        String pageBitsetString = conf.get(property);
        ImmutableRoaringBitmap bitmap = null;

        if (pageBitsetString != null) {
            ByteBuffer buf = ByteBuffer.wrap(pageBitsetString.getBytes("ISO-8859-1"));
            bitmap = new ImmutableRoaringBitmap(buf);
        }
        return bitmap;
    }

    private ImmutableRoaringBitmap readBitmapSet(Path hashKey, String property) throws IOException {
        String pageBitsetMapString = conf.get(property);
        ImmutableRoaringBitmap bitmap = null;

        if (pageBitsetMapString != null) {
            ByteArrayInputStream bais = new ByteArrayInputStream(pageBitsetMapString.getBytes("ISO-8859-1"));
            HashMap<String, SerializableImmutableRoaringBitmap> bitsetMap = null;
            try {
                bitsetMap = (HashMap<String, SerializableImmutableRoaringBitmap>) (new ObjectInputStream(bais).readObject());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            if (bitsetMap != null) {
                bitmap = bitsetMap.get(hashKey.toString()).getBitmap();
            }
        }
        return bitmap;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();
        if (data == null) {
            return false;
        }

        setVal(data);
        return true;
    }

    // We put all columns in values and keep key empty
    private void setVal(List<Object> data) {
        int offset = 0;
        for (int i = 0; i < data.size(); ++i) {
            byte[] src = ((Binary) data.get(i)).getBytes();
            System.arraycopy(src, 0, val, offset, src.length);
            offset += src.length;
        }
    }

    @Override
    public byte[] getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public byte[] getCurrentValue() throws IOException, InterruptedException {
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
