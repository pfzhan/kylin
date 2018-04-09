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

package io.kyligence.kap.engine.spark.storage.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;

public class NParquetCuboidInputFormat extends FileInputFormat<Text, Text> {
    public static final Logger logger = LoggerFactory.getLogger(NParquetCuboidInputFormat.class);

    @Override
    public RecordReader<Text, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new NParquetCuboidReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class NParquetCuboidReader extends RecordReader<Text, Text> {

        protected Configuration conf;

        private List<Path> shardPath;
        private int shardIndex = 0;
        private ParquetBundleReader reader = null;

        private Text key = new Text();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            shardPath = new ArrayList<>();
            shardPath.add(path);

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            int segmentID = Integer.valueOf(context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID));

            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);

            // init with first shard file
            getNextValuesReader();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (reader == null)
                return false;

            List<Object> data = reader.read();
            if (data == null) {
                if (!getNextValuesReader())
                    return false;

                data = reader.read();
                if (data == null)
                    return false;
            }

            setKey(data);
            return true;
        }

        private void setKey(List<Object> data) {
            int valueBytesLength = 0;
            for (Object aData : data) {
                valueBytesLength += ((Binary) aData).getBytes().length;
            }
            byte[] valueBytes = new byte[valueBytesLength];

            int offset = 0;
            for (Object aData : data) {
                byte[] src = ((Binary) aData).getBytes();
                System.arraycopy(src, 0, valueBytes, offset, src.length);
                offset += src.length;
            }

            key.set(valueBytes);
        }

        private boolean getNextValuesReader() throws IOException {
            if (shardIndex < shardPath.size()) {
                if (reader != null) {
                    reader.close();
                }

                Path currPath = shardPath.get(shardIndex);
                FileSystem fileSystem = currPath.getFileSystem(conf);
                try (FSDataInputStream inputStream = fileSystem.open(currPath)) {
                    long fileOffset = inputStream.readLong(); //read the offset
                    reader = new ParquetBundleReader.Builder().setFileOffset(fileOffset).setConf(conf)
                            .setPath(shardPath.get(shardIndex)).build();

                    shardIndex++;
                    return true;
                }
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) shardIndex / shardPath.size();
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
