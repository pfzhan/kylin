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

package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;

public class ParquetTarballMapper extends KylinMapper<IntWritable, byte[], Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetTarballMapper.class);

    private int counter;
    private FSDataOutputStream os;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
        super.bindCurrentConfiguration(conf);
        HadoopUtil.getCurrentConfiguration().setInt("dfs.blocksize", 268435456);

        FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());

        String shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));

        Path invPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");
        long invLength = fs.getFileStatus(invPath).getLen();

        // write to same dir with input
        Path outputPath = new Path(inputPath.getParent(), shardId + ".parquettar");

        logger.info("Input path: " + inputPath.toString());
        logger.info("Output path: " + outputPath.toString());

        os = fs.create(outputPath);
        assert Longs.BYTES == ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE;
        os.writeLong(Longs.BYTES + invLength);
    }

    @Override
    public void map(IntWritable key, byte[] value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }

        os.write(value, 0, key.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        os.close();
    }
}
