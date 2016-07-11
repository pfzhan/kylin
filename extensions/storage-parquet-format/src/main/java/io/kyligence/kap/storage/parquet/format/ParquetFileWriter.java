package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.measure.MeasureDecoder;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriterBuilder;

public class ParquetFileWriter<K, V> extends RecordWriter<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetFileWriter.class);

    private Class<?> keyClass;
    private Class<?> valueClass;

    private String curCubeId = null;
    private String curSegmentId = null;
    private long curCuboidId = 0;
    private short curShardId = 0;

    private Configuration config;
    private KylinConfig kylinConfig;
    private RowKeyDecoder rowKeyDecoder;
    private MeasureDecoder measureDecoder;
    private CubeInstance cubeInstance;
    private CubeSegment cubeSegment;
    private ParquetRawWriter writer = null;
    private String outputDir = null;

    public ParquetFileWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.config = context.getConfiguration();

        curCubeId = config.get(ParquetFormatConstants.KYLIN_CUBE_ID);
        curSegmentId = config.get(ParquetFormatConstants.KYLIN_SEGMENT_ID);
        outputDir = config.get(ParquetFormatConstants.KYLIN_OUTPUT_DIR);

        kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cubeInstance = CubeManager.getInstance(kylinConfig).getCubeByUuid(curCubeId);
        cubeSegment = cubeInstance.getSegmentById(curSegmentId);
        rowKeyDecoder = new RowKeyDecoder(cubeSegment);
        measureDecoder = new MeasureDecoder(cubeSegment.getCubeDesc().getMeasures());

        if (keyClass != Text.class || valueClass != Text.class) {
            throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
        }
    }

    // Only support Text type
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
        byte[] keyBytes = ((Text) key).getBytes().clone();
        byte[] valueBytes = ((Text) value).getBytes().clone();
        long cuboidId = rowKeyDecoder.decode(keyBytes);
        short shardId = rowKeyDecoder.getRowKeySplitter().getShardId();

        if (shardId != curShardId || cuboidId != curCuboidId) {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            curShardId = shardId;
            curCuboidId = cuboidId;
        }

        if (writer == null) {
            List<Type> types = new ArrayList<Type>();

            //TODO: row key is fixed length, will it help?
            types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "Row Key"));

            // measures
            for (MeasureDesc measure : cubeSegment.getCubeDesc().getMeasures()) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, measure.getName()));
            }

            MessageType schema = new MessageType(cubeSegment.getName(), types);
            writer = new ParquetRawWriterBuilder().setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage()).setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression()).setConf(config).setType(schema).setPath(getPath()).build();
        }

        // write data
        try {
            //TODO: why encode again?
            SplittedBytes[] keyBuffers = rowKeyDecoder.getRowKeySplitter().getSplitBuffers();
            int keyLength = 0;
            for (int i = 2; i < rowKeyDecoder.getRowKeySplitter().getBufferSize(); ++i) {
                keyLength += keyBuffers[i].length;
            }
            int keyOffSet = rowKeyDecoder.getRowKeySplitter().getSplitOffsets()[0];

            int[] valueLength = measureDecoder.getPeekLength(ByteBuffer.wrap(valueBytes));
            writer.writeRow(keyBytes, keyOffSet, keyLength, valueBytes, valueLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (writer != null) {
            writer.close();
        }
    }

    private Path getPath() {
        Path path = new Path(new StringBuffer().append(outputDir).append(curCuboidId).append("/").append(curShardId).append(".parquet").toString());
        return path;
    }
}
