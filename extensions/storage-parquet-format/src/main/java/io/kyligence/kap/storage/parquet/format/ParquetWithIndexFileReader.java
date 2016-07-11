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

public class ParquetWithIndexFileReader extends RecordReader<IntWritable, byte[]> {
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
            shardIndexIS.close();
            shardIndexIS = null;
        }

        int b = shardIS.read(val, a, val.length - a);

        if (b < 0) {
            b = 0;
            shardIS.close();
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
