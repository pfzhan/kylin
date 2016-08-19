package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureDecoder;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriterBuilder;

public class ParquetCubeFileWriter extends ParquetOrderedFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetCubeFileWriter.class);

    private long curCuboidId = 0;
    private short curShardId = 0;

    private Configuration config;
    private KylinConfig kylinConfig;
    private MeasureDecoder measureDecoder;
    private CubeInstance cubeInstance;
    private CubeSegment cubeSegment;
    private String outputDir = null;

    public ParquetCubeFileWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException {
        this.config = context.getConfiguration();

        outputDir = config.get(ParquetFormatConstants.KYLIN_OUTPUT_DIR);
        kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
        cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        cubeSegment = cubeInstance.getSegmentById(segmentID);
        Preconditions.checkState(cubeSegment.isEnableSharding(), "Cube segment sharding not enabled " + cubeSegment.getName());

        measureDecoder = new MeasureDecoder(cubeSegment.getCubeDesc().getMeasures());

        if (keyClass == Text.class && valueClass == Text.class) {
            logger.info("KV class is Text");
        } else {
            throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
        }
    }

    @Override
    protected boolean needCutUp(Text key, Text value) {
        byte[] keyBytes = key.getBytes();
        long cuboidId = Bytes.toLong(keyBytes, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
        short shardId = Bytes.toShort(keyBytes, 0, RowConstants.ROWKEY_SHARDID_LEN);

        if (shardId != curShardId || cuboidId != curCuboidId) {
            curShardId = shardId;
            curCuboidId = cuboidId;
            return true;
        }

        if (super.needCutUp(key, value)) {
            return true;
        }

        return false;
    }

    @Override
    protected ParquetRawWriter newWriter() throws IOException {
        ParquetRawWriter rawWriter = null;
        List<Type> types = new ArrayList<Type>();

        //types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, key.getLength() - RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, "Row Key"));
        types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "Row Key"));

        // measures
        for (MeasureDesc measure : cubeSegment.getCubeDesc().getMeasures()) {
            types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, measure.getName()));
        }

        MessageType schema = new MessageType(cubeSegment.getName(), types);
        rawWriter = new ParquetRawWriterBuilder().setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage()).setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression()).setConf(config).setType(schema).setPath(getPath()).build();
        return rawWriter;
    }

    @Override
    protected void writeData(Text key, Text value) {
        byte[] valueBytes = value.getBytes().clone(); //on purpose, because parquet writer will cache
        try {
            byte[] keyBody = Arrays.copyOfRange(key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, key.getLength());
            int[] valueLength = measureDecoder.getPeekLength(ByteBuffer.wrap(valueBytes));
            writer.writeRow(keyBody, 0, keyBody.length, valueBytes, valueLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Path getPath() {
        Path path = new Path(new StringBuffer().append(outputDir).append(curCuboidId).append("/").append(curShardId).append(".parquet").toString());
        return path;
    }
}
