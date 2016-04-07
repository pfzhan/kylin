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

package io.kyligence.kap.engine.mr;

import io.kyligence.kap.cube.GTColumnForwardIndex;
import io.kyligence.kap.cube.GTColumnInvertedIndex;
import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
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
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;

/**
 */
public class SecondaryIndexReducer extends KylinReducer<Text, Text, NullWritable, Text> {

    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    private TblColRef col = null;
    private int colOffset;
    private int colLength;
    private IColumnForwardIndex.Builder forwardIndexBuilder;
    private IColumnInvertedIndex.Builder invertedIndexBuilder;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        final String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        final String segmentName = conf.get(BatchConstants.CFG_CUBE_SEGMENT_NAME);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        RowKeyColumnIO colIO = new RowKeyColumnIO(new CubeDimEncMap(cubeSegment));
        int taskId = context.getTaskAttemptID().getTaskID().getId();

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        col = baseCuboid.getColumns().get(taskId);
        colLength = colIO.getColumnLength(col);

        colOffset = SecondaryIndexMapper.COLUMN_ID_LENGTH;
        for (int i = 0; i < taskId; i++) {
            colOffset += colIO.getColumnLength(baseCuboid.getColumns().get(i));
        }

        forwardIndexBuilder = new GTColumnForwardIndex(cubeSegment, col).rebuild();
        invertedIndexBuilder = new GTColumnInvertedIndex(cubeSegment, col).rebuild();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int value = BytesUtil.readUnsigned(key.getBytes(), colOffset, colLength);
        //write the value to the index files
        forwardIndexBuilder.putNextRow(value);
        invertedIndexBuilder.putNextRow(value);
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // upload to hdfs
        forwardIndexBuilder.close();
        invertedIndexBuilder.close();

        FileSystem fs = FileSystem.get(context.getConfiguration());

        try {
            Path path = new Path(cubeSegment.getIndexPath());
            fs.delete(path, true);
            fs.mkdirs(path);

            fs.copyFromLocalFile(true, new Path(""), path);

        } finally {
            fs.close();
        }

    }

}
