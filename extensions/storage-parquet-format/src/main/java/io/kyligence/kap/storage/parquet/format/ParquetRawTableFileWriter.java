package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.kyligence.kap.raw.BufferedRawEncoder;
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
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.kv.RawTableConstants;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriterBuilder;

public class ParquetRawTableFileWriter extends ParquetOrderedFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRawTableFileWriter.class);

    private short curShardId = 0;

    private Configuration config;
    private KylinConfig kylinConfig;
    private CubeInstance cubeInstance;
    private CubeSegment cubeSegment;
    private RawTableDesc rawTableDesc;
    private BufferedRawEncoder rawEncoder;
    private String outputDir = null;

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        super.write(key, value);
    }

    public ParquetRawTableFileWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) throws IOException {
        this.config = context.getConfiguration();

        kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

        outputDir = config.get(ParquetFormatConstants.KYLIN_OUTPUT_DIR);
        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
        cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        cubeSegment = cubeInstance.getSegmentById(segmentID);

        new RawTableInstance(cubeInstance);

        rawTableDesc = new RawTableDesc(cubeInstance.getDescriptor());
        rawEncoder = new BufferedRawEncoder(rawTableDesc.getColumnsExcludingOrdered());

        // FIXME: Text involves array copy every time
        if (keyClass == Text.class && valueClass == Text.class) {
            logger.info("KV class is Text");
        } else {
            throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
        }
    }

    @Override
    protected boolean needCutUp(Text key, Text value) {
        byte[] keyByte = key.getBytes();
        short shardId = Bytes.toShort(keyByte, 0, RawTableConstants.SHARDID_LEN);

        if (shardId != curShardId) {
            curShardId = shardId;
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
        TblColRef orderedColumn = rawTableDesc.getOrderedColumn();
        List<TblColRef> columns = rawTableDesc.getColumns();

        types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, orderedColumn.getName()));
        for (TblColRef column : columns) {
            if (!column.equals(orderedColumn)) {
                types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, column.getName()));
            }
        }

        MessageType schema = new MessageType(rawTableDesc.getName(), types);
        rawWriter = new ParquetRawWriterBuilder().setRowsPerPage(KapConfig.getInstanceFromEnv().getParquetRowsPerPage()).setCodecName(KapConfig.getInstanceFromEnv().getParquetPageCompression()).setConf(config).setType(schema).setPath(getPath()).build();

        return rawWriter;
    }

    @Override
    protected void writeData(Text key, Text value) {
        byte[] valueBytes = value.getBytes().clone(); //on purpose, because parquet writer will cache
        try {
            byte[] keyBody = Arrays.copyOfRange(key.getBytes(), RawTableConstants.SHARDID_LEN, key.getLength());
            int[] valueLength = rawEncoder.peekLength(ByteBuffer.wrap(valueBytes));
            writer.writeRow(keyBody, 0, keyBody.length, valueBytes, valueLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Path getPath() {
        Path path = new Path(new StringBuffer().append(outputDir).append(RawTableConstants.RawTableDir).append("/").append(curShardId).append(".parquet").toString());
        return path;
    }
}
