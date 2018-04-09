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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;

public class ParquetCubeInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetCubeReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetCubeReader extends RecordReader<Text, Text> {
        public static final Logger logger = LoggerFactory.getLogger(ParquetCubeReader.class);

        protected Configuration conf;

        private long curCuboidId;

        private KylinConfig kylinConfig;
        private CubeInstance cubeInstance;
        private CubeSegment cubeSegment;
        private RowKeyEncoder rowKeyEncoder;

        private List<Path> shardPath;
        private int shardIndex = 0;
        private ParquetBundleReader reader = null;

        private Text key = new Text();
        private Text val = new Text();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            shardPath = new ArrayList<>();
            shardPath.add(path);
            kylinConfig = KylinConfig.loadKylinPropsAndMetadata();

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
            cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            cubeSegment = cubeInstance.getSegmentById(segmentID);

            // init with first shard file
            reader = getNextValuesReader();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            List<Object> data = reader.read();

            if (data == null) {
                reader = getNextValuesReader();
                if (reader == null) {
                    return false;
                }

                data = reader.read();
                if (data == null) {
                    return false;
                }
            }

            // key
            byte[] keyBytes = ((Binary) data.get(0)).getBytes();
            ByteArray keyByteArray = new ByteArray(keyBytes.length + RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN);
            rowKeyEncoder.encode(new ByteArray(keyBytes), keyByteArray);
            key.set(keyByteArray.array());

            // value
            setVal(data);

            return true;
        }

        private void setVal(List<Object> data) {
            int valueBytesLength = 0;
            for (int i = 1; i < data.size(); ++i) {
                valueBytesLength += ((Binary) data.get(i)).getBytes().length;
            }
            byte[] valueBytes = new byte[valueBytesLength];

            int offset = 0;
            for (int i = 1; i < data.size(); ++i) {
                byte[] src = ((Binary) data.get(i)).getBytes();
                System.arraycopy(src, 0, valueBytes, offset, src.length);
                offset += src.length;
            }

            val.set(valueBytes);
        }

        private void setCurrentCuboidShard(Path path) {
            String[] dirs = path.toString().split("/");
            curCuboidId = Long.parseLong(dirs[dirs.length - 2]);
        }

        private ParquetBundleReader getNextValuesReader() throws IOException {
            if (shardIndex < shardPath.size()) {
                if (reader != null) {
                    reader.close();
                }
                reader = new ParquetBundleReader.Builder().setConf(conf).setPath(shardPath.get(shardIndex)).build();

                setCurrentCuboidShard(shardPath.get(shardIndex));
                rowKeyEncoder = new RowKeyEncoder(cubeSegment, Cuboid.findById(cubeInstance, curCuboidId));
                shardIndex++;
                return reader;
            }
            return null;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return val;
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
