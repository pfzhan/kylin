package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DateStrDictionary;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;

/**
 * Created by dongli on 5/31/16.
 */
public class ParquetPageIndexMapper extends KylinMapper<Text, IntWritable, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexMapper.class);

    protected String cubeName;
    protected String segmentName;
    protected long cuboidId;
    protected String shardId;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected Cuboid cuboid;

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
        cuboidId = Long.parseLong(inputPath.getParent().getName());
        shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        // write to same dir with input
        outputPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cuboid = Cuboid.findById(cubeDesc, cuboidId);
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());

        initIndexWriters();
    }

    private void initIndexWriters() throws IOException {
        RowKeyEncoder rowKeyEncoder = (RowKeyEncoder) AbstractRowKeyEncoder.createInstance(cubeSegment, cuboid);

        int columnNum = cuboid.getColumns().size();
        columnLength = new int[columnNum];
        cardinality = new int[columnNum];
        columnName = new String[columnNum];
        onlyEQIndex = new boolean[columnNum]; // should get from rowKey.index

        for (int col = 0; col < columnNum; col++) {
            TblColRef colRef = cuboid.getColumns().get(col);
            int colCardinality = -1;
            Dictionary<String> dict = cubeSegment.getDictionary(colRef);
            // TODO: all column has eq/gt/lt index now
            onlyEQIndex[col] = false;

            if (dict != null) {
                colCardinality = dict.getSize();
                if (dict instanceof DateStrDictionary) {
                    colCardinality = -1;
                    onlyEQIndex[col] = false;
                }
            }

            cardinality[col] = colCardinality;
            columnLength[col] = rowKeyEncoder.getColumnLength(colRef);
            columnName[col] = colRef.getName();

            logger.debug("Column Length:" + columnName[col] + "=" + columnLength[col]);
        }

        FSDataOutputStream outputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(outputPath);
        indexBundleWriter = new ParquetPageIndexWriter(columnName, columnLength, cardinality, onlyEQIndex, outputStream);
    }

    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }
        indexBundleWriter.write(key.getBytes(), value.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        indexBundleWriter.close();
    }
}
