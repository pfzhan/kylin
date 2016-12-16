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
import java.util.List;

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

import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexSpliceWriter;

public class ParquetSplicePageIndexMapper extends KylinMapper<ByteArrayListWritable, IntWritable, Text, IntWritable> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetSplicePageIndexMapper.class);

    protected String cubeName;
    protected String segmentID;
    protected long curCuboidId = -1;
    protected short curShardId = -1;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected Cuboid cuboid;

    private ParquetPageIndexSpliceWriter indexSpliceWriter;
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

        outputPath = new Path(FileOutputFormat.getWorkOutputPath(context), inputPath.getName().replace("parquet", ".parquet.inv"));
        FSDataOutputStream outputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).create(outputPath);
        indexSpliceWriter = new ParquetPageIndexSpliceWriter(outputStream);

        // cube and its segment keep the same in the mapper
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);

        logger.info("Input path: " + inputPath.toUri().toString());
        logger.info("Output path: " + outputPath.toString());
    }

    @Override
    public void doMap(ByteArrayListWritable key, IntWritable value, Context context) throws IOException {
        List<byte[]> keys = key.get();
        String[] div = String.valueOf(keys.get(0)).split("-");
        if (Long.valueOf(div[0]) != curCuboidId || Short.valueOf(div[1]) != curShardId) {
            curCuboidId = Long.valueOf(div[0]);
            curShardId = Short.valueOf(div[1]);
            refreshWriter();
        }

        // clone byte array to avoid object reuse effect
        indexSpliceWriter.write(keys.get(1).clone(), value.get());
    }

    private void refreshWriter() throws IOException {
        if (indexSpliceWriter.isDivStarted()) {
            indexSpliceWriter.endDiv();
        }

        cuboid = Cuboid.findById(cubeDesc, curCuboidId);
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

        indexSpliceWriter.startDiv(curCuboidId + "-" + curShardId, columnName, columnLength, cardinality, onlyEQIndex);
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        indexSpliceWriter.close();
    }
}
