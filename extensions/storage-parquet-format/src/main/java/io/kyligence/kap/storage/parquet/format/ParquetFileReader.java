package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.parquet.io.api.Binary;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;

public class ParquetFileReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private String curCubeId = null;
    private String curSegmentId = null;

    private long curCuboidId;

    private KylinConfig kylinConfig;
    private CubeInstance cubeInstance;
    private CubeSegment cubeSegment;
    private RowKeyEncoder rowKeyEncoder;

    private List<Path> shardPath;
    private int shardIndex = 0;
    private ParquetBundleReader reader = null;

    private K key;
    private V val;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        shardPath = new ArrayList<>();
        shardPath.add(path);

        curCubeId = conf.get(ParquetFormatConstants.KYLIN_CUBE_ID);
        curSegmentId = conf.get(ParquetFormatConstants.KYLIN_SEGMENT_ID);

        kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cubeInstance = CubeManager.getInstance(kylinConfig).getCubeByUuid(curCubeId);
        cubeSegment = cubeInstance.getSegmentById(curSegmentId);

        // init with first shard file
        reader = getNextValuesReader();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        List<Object> data = reader.read();

        if (data == null) {
            reader = getNextValuesReader();
            if (reader == null) {
                return false;
            }

            data = reader.read();
            if (data == null) {
                return false;
            }
        }

        // key
        byte[] keyBytes = ((Binary) data.get(0)).getBytes();
        ByteArray keyByteArray = new ByteArray(keyBytes.length + 10);
        rowKeyEncoder.encode(new ByteArray(keyBytes), keyByteArray);
        key = (K) new Text(keyByteArray.array());

        // value
        setVal(data);

        return true;
    }

    private void setVal(List<Object> data) {
        int valueBytesLength = 0;
        for (int i = 1; i < data.size(); ++i) {
            valueBytesLength += ((Binary) data.get(i)).getBytes().length;
        }
        byte[] valueBytes = new byte[valueBytesLength];

        int offset = 0;
        for (int i = 1; i < data.size(); ++i) {
            byte[] src = ((Binary) data.get(i)).getBytes();
            System.arraycopy(src, 0, valueBytes, offset, src.length);
            offset += src.length;
        }

        val = (V) new Text(valueBytes);
    }

    private void setCurrentCuboidShard(Path path) {
        String[] dirs = path.toString().split("/");
        curCuboidId = Long.parseLong(dirs[dirs.length - 2]);
    }

    private ParquetBundleReader getNextValuesReader() throws IOException {
        if (shardIndex < shardPath.size()) {
            if (reader != null) {
                reader.close();
            }
            reader = new ParquetBundleReaderBuilder().setConf(conf).setPath(shardPath.get(shardIndex)).build();

            setCurrentCuboidShard(shardPath.get(shardIndex));
            rowKeyEncoder = new RowKeyEncoder(cubeSegment, Cuboid.findById(cubeInstance.getDescriptor(), curCuboidId));
            shardIndex++;
            return reader;
        }
        return null;
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
        return (float) shardIndex / shardPath.size();
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
