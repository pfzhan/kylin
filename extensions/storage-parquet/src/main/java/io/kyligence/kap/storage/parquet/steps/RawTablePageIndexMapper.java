package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;

public class RawTablePageIndexMapper extends KylinMapper<ByteArrayListWritable, IntWritable, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(RawTablePageIndexMapper.class);

    protected String cubeName;
    protected String segmentName;
    protected String shardId;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected RawTableInstance rawTableInstance;
    protected RawTableDesc rawTableDesc;

    private ParquetPageIndexWriter indexBundleWriter;
    private int counter = 0;
    private Path outputPath;

    private int[] columnLength;
    private int[] cardinality;
    private String[] columnName;
    private boolean[] onlyEQIndex;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path inputPath = ((FileSplit) context.getInputSplit()).getPath();

        super.bindCurrentConfiguration(conf);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        // write to same dir with input
        outputPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        rawTableInstance = RawTableManager.getInstance(config).getRawTableInstance(cubeName);
        rawTableDesc = rawTableInstance.getRawTableDesc();

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());

        initIndexWriters();
    }

    private void initIndexWriters() throws IOException {
        //        RowKeyEncoder rowKeyEncoder = (RowKeyEncoder) AbstractRowKeyEncoder.createInstance(cubeSegment, cuboid);

        int columnNum = rawTableDesc.getColumns().size();
        columnLength = new int[columnNum];
        cardinality = new int[columnNum];
        columnName = new String[columnNum];
        onlyEQIndex = new boolean[columnNum]; // should get from rowKey.index

        List<TblColRef> columns = rawTableDesc.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            columnLength[i] = 8;
            onlyEQIndex[i] = true;
            cardinality[i] = 10000;
            columnName[i] = column.getName();

        }

        FSDataOutputStream outputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(outputPath);
        indexBundleWriter = new ParquetPageIndexWriter(columnName, columnLength, cardinality, onlyEQIndex, outputStream);
    }

    @Override
    public void map(ByteArrayListWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }
        List<byte[]> hashedValue = hash(key.get());
        indexBundleWriter.write(hashedValue, value.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        indexBundleWriter.close();
    }

    private List<byte[]> hash(List<byte[]> value) {
        List<byte[]> result = new ArrayList<>(value.size());
        for (byte[] v: value) {
            result.add(hash(v));
        }
        return result;
    }

    private byte[] hash(byte[] value) {
        byte[] result = new byte[8];

        if (value.length <= 8) {
            System.arraycopy(value, 0, result, 0, value.length);
            for (int i = value.length; i < 8; i++) {
                result[i] = (byte)0;
            }
        } else {
            System.arraycopy(value, 0, result, 0, 8);

            for (int i = 8; i < value.length; i++) {
                result[i % 8] ^= value[i];
            }
        }

        return result;
    }

    private List<Integer> getExtendedColumnLen() {
        List<Integer> lens = new ArrayList<>();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (measure.getFunction().getExpression().equalsIgnoreCase("EXTENDED_COLUMN")) {
                lens.add(Integer.parseInt(measure.getFunction().getReturnType().split("\\(|\\)")[2]));
            }
        }

        return lens;
    }
}
