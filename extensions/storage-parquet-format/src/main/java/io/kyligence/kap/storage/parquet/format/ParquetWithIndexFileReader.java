/**
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

package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetWithIndexFileReader extends RecordReader<IntWritable, byte[]> {

    protected static final Logger logger = LoggerFactory.getLogger(ParquetWithIndexFileReader.class);

    protected Configuration conf;

    private FSDataInputStream shardIS;
    private FSDataInputStream shardIndexIS;

    private IntWritable key = new IntWritable(0);
    private byte[] val;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path shardPath = fileSplit.getPath();
        String shardIndexPathString = shardPath.toString().replace(".parquet", ".parquet.inv");
        Path shardIndexPath = new Path(shardIndexPathString);
        val = new byte[128 << 10];

        shardIS = FileSystem.get(conf).open(shardPath);
        shardIndexIS = FileSystem.get(conf).open(shardIndexPath);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        int a = 0;
        if (shardIndexIS != null) {
            a = shardIndexIS.read(val, 0, val.length);
        }

        if (a < 0) {
            a = 0;
            logger.info("closing shardIndexIS");
            shardIndexIS.close();
            shardIndexIS = null;
        }

        int b = 0;
        if (shardIS != null) {
            b = shardIS.read(val, a, val.length - a);
        }

        if (b < 0) {
            b = 0;
            logger.info("closing shardIS");
            shardIS.close();
            shardIS = null;
        }
        key.set(a + b);
        return key.get() > 0;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public byte[] getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
