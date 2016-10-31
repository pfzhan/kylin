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

package io.kyligence.kap.engine.mr.index;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.ColumnIndexWriter;

/**
 */
public class SecondaryIndexReducer extends KylinReducer<Text, Text, NullWritable, Text> {
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexReducer.class);
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    private TblColRef col = null;
    private int colOffset;
    private int colLength;
    private File forwardIndexFile;
    private File invertedIndexFile;
    private String outputPath;

    private ColumnIndexWriter columnIndexWriter;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        final String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        final String segmentID = conf.get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        outputPath = conf.get(BatchConstants.CFG_OUTPUT_PATH);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);
        RowKeyColumnIO colIO = new RowKeyColumnIO(new CubeDimEncMap(cubeSegment));
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        int colIndexInRowKey = cube.getDescriptor().getRowkey().getColumnsNeedIndex()[taskId];

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        col = baseCuboid.getColumns().get(colIndexInRowKey);
        colLength = colIO.getColumnLength(col);

        colOffset = SecondaryIndexMapper.COLUMN_ID_LENGTH;
        for (int i = 0; i < colIndexInRowKey; i++) {
            colOffset += colIO.getColumnLength(baseCuboid.getColumns().get(i));
        }

        forwardIndexFile = File.createTempFile(col.getName(), ".fwd");
        invertedIndexFile = File.createTempFile(col.getName(), ".inv");
        columnIndexWriter = new ColumnIndexWriter(col, cubeSegment.getDictionary(col), colOffset, colLength, forwardIndexFile, invertedIndexFile);

    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        columnIndexWriter.write(key.getBytes());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        columnIndexWriter.close();

        // upload to hdfs
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(outputPath);
        fs.mkdirs(path);
        fs.copyFromLocalFile(true, new Path(forwardIndexFile.toURI()), new Path(path, col.getName() + ".fwd"));
        fs.copyFromLocalFile(true, new Path(invertedIndexFile.toURI()), new Path(path, col.getName() + ".inv"));

        forwardIndexFile.delete();
        invertedIndexFile.delete();
    }

}
