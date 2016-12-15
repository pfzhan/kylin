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

package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.io.api.Binary;

import com.google.common.collect.Lists;

import io.kyligence.kap.storage.parquet.format.datatype.ByteArrayListWritable;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceReader;
import io.kyligence.kap.storage.parquet.format.file.Utils;

/**
 * Used to build parquet inverted index
 * @param <K> Dimension values
 * @param <V> Page Id
 */
public class ParquetCubeSplicePageInputFormat<K, V> extends FileInputFormat<K, V> {
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetCubePageReader<>();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetCubePageReader<K, V> extends RecordReader<K, V> {
        protected Configuration conf;

        private Path path;
        private ParquetSpliceReader spliceReader = null;
        private ParquetBundleReader reader = null;
        private List<String> divs;
        private int divIndex = 0;
        private String curDiv;

        private K key;
        private V val;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            path = fileSplit.getPath();

            // init with first shard file
            spliceReader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path).setColumnsBitmap(Utils.createBitset(1)).build();
            divs = Lists.newArrayList(spliceReader.getDivs());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (null == reader && !getNextReader()) {
                return false;
            }

            Binary keyBytes = (Binary) reader.read().get(0);
            if (keyBytes == null) {
                if (!getNextReader()) {
                    return false;
                }
                return nextKeyValue();
            }

            // Key = curDiv + origin key
            key = (K) new ByteArrayListWritable(curDiv.getBytes(), keyBytes.getBytes());
            val = (V) new IntWritable(reader.getPageIndex());
            return true;
        }

        private boolean getNextReader() throws IOException {
            if (divIndex < divs.size()) {
                if (reader != null) {
                    reader.close();
                }
                curDiv = divs.get(divIndex++);
                reader = spliceReader.getDivReader(curDiv);
                return true;
            }
            return false;
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
}