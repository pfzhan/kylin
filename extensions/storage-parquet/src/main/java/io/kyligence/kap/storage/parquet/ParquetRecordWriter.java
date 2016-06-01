package io.kyligence.kap.storage.parquet;

import io.kyligence.kap.engine.mr.common.KapBatchConstants;
import io.kyligence.kap.storage.parquet.file.ParquetWriter;
import io.kyligence.kap.storage.parquet.file.ParquetWriterBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.measure.MeasureDecoder;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

public class ParquetRecordWriter <K,V> extends RecordWriter<K, V>{
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
    private ParquetWriter writer = null;

    public ParquetRecordWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.config = context.getConfiguration();

        curCubeId = config.get(KapBatchConstants.KYLIN_CUBE_ID);
        curSegmentId = config.get(KapBatchConstants.KYLIN_SEGMENT_ID);

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
        byte[] keyBytes = ((Text)key).getBytes();
        byte[] valueBytes = ((Text)value).getBytes();
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

            // dimension
            for (TblColRef column : rowKeyDecoder.getColumns()) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, column.getName()));
            }

            // measures
            for (MeasureDesc measure : cubeSegment.getCubeDesc().getMeasures()) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, measure.getName()));
            }

            MessageType schema = new MessageType(cubeSegment.getName(), types);
            writer = new ParquetWriterBuilder()
                    .setConf(config)
                    .setType(schema)
                    .setPath(getPath())
                    .build();
        }

        // write data
        writer.writeRow(keyBytes, rowKeyDecoder.getRowKeySplitter().getSplitOffsets(), valueBytes, measureDecoder.getPeekLength(ByteBuffer.wrap(valueBytes)));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (writer != null) {
            writer.close();
        }
    }

    private Path getPath() {
        return new Path(new StringBuffer(kylinConfig.getHdfsWorkingDirectory())
                .append("parquet/")
                .append(curCubeId).append("/")
                .append(curSegmentId).append("/")
                .append(curCuboidId).append("/")
                .append(curShardId).append(".parquet")
                .toString());
    }
}
