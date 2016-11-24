/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;

public class ParquetPageIndexMapper extends KylinMapper<Text, IntWritable, Text, IntWritable> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexMapper.class);

    protected String cubeName;
    protected String segmentID;
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
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path inputPath = ((FileSplit) context.getInputSplit()).getPath();

        super.bindCurrentConfiguration(conf);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        if (inputPath.getParent().getName().matches("[0-9]+"))
            cuboidId = Long.parseLong(inputPath.getParent().getName());
        shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        outputPath = new Path(FileOutputFormat.getWorkOutputPath(context), cuboidId + "/" + shardId + ".parquet.inv");

        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cuboid = Cuboid.findById(cubeDesc, cuboidId);
        cubeSegment = cube.getSegmentById(segmentID);

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());

        initIndexWriters(context);
    }

    private void initIndexWriters(Context context) throws IOException, InterruptedException {
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

            String rowKeyIndexType = cubeDesc.getRowkey().getColDesc(colRef).getIndex();
            if ("eq".equalsIgnoreCase(rowKeyIndexType)) {
                onlyEQIndex[col] = true;
            } else {
                onlyEQIndex[col] = false;
            }

            if (dict != null) {
                colCardinality = dict.getSize();
                if (dict instanceof DateStrDictionary) {
                    colCardinality = -1;
                    //                                    onlyEQIndex[col] = false;
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
    public void doMap(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }
        indexBundleWriter.write(key.getBytes(), value.get());
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        indexBundleWriter.close();
    }
}
