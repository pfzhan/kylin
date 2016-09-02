package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MemoryBudgetController;
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
import io.kyligence.kap.raw.BufferedRawEncoder;
import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;
import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

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

    private Path inputPath;
    private ParquetPageIndexWriter indexBundleWriter;
    private HashMap<Integer, ParquetPageIndexWriter> fuzzyIndexWriterMap;
    private HashMap<Integer, BufferedRawEncoder> fuzzyIndexEncodingMap;
    private int counter = 0;
    private int fuzzyLength = 0;
    private Path outputPath;

    private ColumnSpec[] columnSpecs;

    private double spillThresholdMB;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        inputPath = ((FileSplit) context.getInputSplit()).getPath();

        super.bindCurrentConfiguration(conf);

        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        KapConfig kapConfig = KapConfig.wrap(AbstractHadoopJob.loadKylinPropsAndMetadata());
        spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

        fuzzyLength = KapConfig.getInstanceFromEnv().getParquetFuzzyIndexLength();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        // write to same dir with input
        outputPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");
        cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        rawTableInstance = RawTableManager.getInstance(kylinConfig).getRawTableInstance(cubeName);
        rawTableDesc = rawTableInstance.getRawTableDesc();

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());

        initIndexWriters();
    }

    private void initIndexWriters() throws IOException {
        int hashLength = KapConfig.getInstanceFromEnv().getParquetIndexHashLength();

        fuzzyIndexWriterMap = new HashMap<>();
        fuzzyIndexEncodingMap = new HashMap<>();
        List<TblColRef> columns = rawTableDesc.getColumns();
        columnSpecs = new ColumnSpec[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            columnSpecs[i] = new ColumnSpec(column.getName(), hashLength, 10000, true, i);
            columnSpecs[i].setValueEncodingIdentifier('s');

            if (rawTableDesc.isNeedFuzzyIndex(column)) {
                Path path = new Path(inputPath.getParent(), shardId + "." + i + ".parquet.fuzzy");
                FSDataOutputStream output = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(path);
                ColumnSpec columnSpec = new ColumnSpec(column.getName(), fuzzyLength, 10000, true, i);
                columnSpec.setValueEncodingIdentifier('s');
                fuzzyIndexWriterMap.put(i, new ParquetPageIndexWriter(new ColumnSpec[] { columnSpec }, output));
                fuzzyIndexEncodingMap.put(i, new BufferedRawEncoder(column));
            }
        }

        FSDataOutputStream outputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(outputPath);
        indexBundleWriter = new ParquetPageIndexWriter(columnSpecs, outputStream);
    }

    @Override
    public void map(ByteArrayListWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        List<byte[]> originValue = key.get();
        List<byte[]> hashedValue = RawTableUtils.hash(originValue);

        for (Integer fuzzyIndex : fuzzyIndexWriterMap.keySet()) {
            ParquetPageIndexWriter writer = fuzzyIndexWriterMap.get(fuzzyIndex);
            String[] decoded = new String[1];
            fuzzyIndexEncodingMap.get(fuzzyIndex).decode(ByteBuffer.wrap(originValue.get(fuzzyIndex)), decoded);
            writeSubstring(writer, decoded[0].getBytes(), value.get(), fuzzyLength);
        }

        indexBundleWriter.write(hashedValue, value.get());

        spillIfNeeded();
    }

    private void writeSubstring(ParquetPageIndexWriter writer, byte[] value, int pageId, int length) {
        // skip if value's length is less than required length
        if (value.length < length) {
            return;
        }

        for (int index = 0; index <= (value.length - length); index++) {
            writer.write(value, index, pageId);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        indexBundleWriter.close();
        for (ParquetPageIndexWriter writer : fuzzyIndexWriterMap.values()) {
            writer.close();
        }
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

    private void spillIfNeeded() {
        long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
        if (availMemoryMB < spillThresholdMB) {
            logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
            indexBundleWriter.spill();
            for (ParquetPageIndexWriter writer : fuzzyIndexWriterMap.values()) {
                writer.spill();
            }
            logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
        }
    }
}
