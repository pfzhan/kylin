/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
