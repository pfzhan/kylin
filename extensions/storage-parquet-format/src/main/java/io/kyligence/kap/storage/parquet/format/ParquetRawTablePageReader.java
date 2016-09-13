package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;

public class ParquetRawTablePageReader<K, V> extends RecordReader<K, V> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetRawTablePageReader.class);

    protected Configuration conf;

    private Path shardPath;
    private ParquetBundleReader reader = null;

    private K key;
    private V val;
    private KylinConfig kylinConfig;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        shardPath = fileSplit.getPath();
        logger.info("shard file path: {}", shardPath.toString());
        // init with first shard file
        reader = new ParquetBundleReaderBuilder().setConf(conf).setPath(shardPath).build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> row = reader.read();

        if (row == null) {
            return false;
        }

        int len = row.size();
        byte[][] columnBytes = new byte[len][];

        for (int i = 0; i < len; i++) {
            columnBytes[i] = ((Binary) row.get(i)).getBytes();
        }

        key = (K) new ByteArrayListWritable(columnBytes);
        val = (V) new IntWritable(reader.getPageIndex());
        return true;
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

    // Return a list present extended columns index,
    // as dimensions is in the first column, add 1 to result index
    private ImmutableRoaringBitmap countExtendedColumn(CubeInstance cube) {
        List<MeasureDesc> measures = cube.getMeasures();
        int len = measures.size();
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i = 0; i < len; ++i) {

            // TODO: wrapper to a util function
            if (measures.get(i).getFunction().getExpression().equalsIgnoreCase("EXTENDED_COLUMN")) {
                bitmap.add(i + 1);
            }
        }

        return bitmap.toImmutableRoaringBitmap();
    }
}
