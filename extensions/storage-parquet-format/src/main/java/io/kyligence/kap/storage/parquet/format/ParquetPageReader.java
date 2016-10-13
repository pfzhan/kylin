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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.io.api.Binary;

import io.kyligence.kap.storage.parquet.format.file.GeneralValuesReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetColumnReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetColumnReaderBuilder;

public class ParquetPageReader<K, V> extends RecordReader<K, V> {
    protected Configuration conf;

    private Path shardPath;
    private ParquetColumnReader reader = null;
    private GeneralValuesReader valuesReader = null;

    private K key;
    private V val;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        shardPath = path;

        // init with first shard file
        reader = new ParquetColumnReaderBuilder().setConf(conf).setPath(shardPath).build();
        valuesReader = reader.getNextValuesReader();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Binary keyBytes = valuesReader.readBytes();
        if (keyBytes == null) {
            valuesReader = reader.getNextValuesReader();
            if (valuesReader == null) {
                return false;
            }
            keyBytes = valuesReader.readBytes();
            if (keyBytes == null) {
                return false;
            }
        }

        key = (K) new Text(keyBytes.getBytes());
        val = (V) new IntWritable(reader.getPageIndex());
        return true;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
