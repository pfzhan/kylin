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
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

public class ParquetTarballMapper extends KylinMapper<IntWritable, byte[], Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetTarballMapper.class);

    private FSDataOutputStream os;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
        super.bindCurrentConfiguration(conf);
        FileSystem fs = HadoopUtil.getFileSystem(inputPath, conf);

        String shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));
        String cuboidId = inputPath.getParent().getName();

        Path invPath = new Path(inputPath.getParent(), shardId + ".parquet.inv");
        long invLength = fs.getFileStatus(invPath).getLen();

        // write to same dir with input
        Path outputPath = new Path(FileOutputFormat.getWorkOutputPath(context), cuboidId + "/" + shardId + ".parquettar");

        logger.info("Input path: " + inputPath.toString());
        logger.info("Output path: " + outputPath.toString());

        // make tar replicate factor to 3, wish to better query performance
        os = fs.create(outputPath, (short) 3);
        os.writeLong(Longs.BYTES + invLength);
    }

    @Override
    public void doMap(IntWritable key, byte[] value, Context context) throws IOException, InterruptedException {
        os.write(value, 0, key.get());
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        os.close();
    }
}
