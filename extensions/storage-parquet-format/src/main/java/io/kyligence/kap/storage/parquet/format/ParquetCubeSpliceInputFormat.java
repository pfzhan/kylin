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

import static io.kyligence.kap.storage.parquet.format.ParquetCubeSpliceOutputFormat.ParquetCubeSpliceWriter.getCuboididFromDiv;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceReader;

public class ParquetCubeSpliceInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetCubeSpliceReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class ParquetCubeSpliceReader extends RecordReader<Text, Text> {
        public static final Logger logger = LoggerFactory.getLogger(ParquetCubeSpliceReader.class);

        protected Configuration conf;

        private KylinConfig kylinConfig;
        private CubeInstance cubeInstance;
        private CubeSegment cubeSegment;
        private RowKeyEncoder rowKeyEncoder;

        private Path path;

        private ParquetSpliceReader spliceReader = null;
        private ParquetBundleReader reader = null;
        private List<String> divs;
        private int divIndex = 0;

        private Text key = new Text();
        private Text val = new Text();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
            path = fileSplit.getPath();
            kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            String[] requiredCuboids;

            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);
            cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            cubeSegment = cubeInstance.getSegmentById(segmentID);
            // init with first shard file
            spliceReader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path).build();

            if (context.getConfiguration().get(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS).equals("All")) {
                // "All" for all divs
                requiredCuboids = getAllCuboidsFromFile();
            } else {
                // <cuboid-id1>,<cuboid-id2>,...,<cuboid-idn>
                requiredCuboids = context.getConfiguration().get(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS).split(",");
            }

            filterDivs(requiredCuboids);
        }

        private String[] getAllCuboidsFromFile() {
            return (String[]) Lists.newArrayList(getCuboid2DivMap().keySet()).toArray();
        }

        private Map<String, List<String>> getCuboid2DivMap() {
            Map<String, List<String>> cuboidDivMap = Maps.newHashMap();
            for (String d : spliceReader.getDivs()) {
                String cuboid = String.valueOf(getCuboididFromDiv(d));
                if (!cuboidDivMap.containsKey(cuboid)) {
                    cuboidDivMap.put(cuboid, Lists.<String> newArrayList());
                }
                cuboidDivMap.get(cuboid).add(d);
            }
            return cuboidDivMap;
        }

        private void filterDivs(String[] requiredCuboids) {
            divs = Lists.newArrayList();
            Map<String, List<String>> cuboidDivMap = getCuboid2DivMap();
            for (String cuboid : requiredCuboids) {
                if (cuboidDivMap.containsKey(cuboid)) {
                    divs.addAll(cuboidDivMap.get(cuboid));
                }
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (null == reader && !getNextReader()) {
                return false;
            }
            List<Object> data = reader.read();

            if (data == null) {
                if (!getNextReader()) {
                    return false;
                }

                return nextKeyValue();
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

        private boolean getNextReader() throws IOException {
            if (divIndex < divs.size()) {
                if (reader != null) {
                    reader.close();
                }
                String div = divs.get(divIndex++);
                reader = spliceReader.getDivReader(div);
                rowKeyEncoder = new RowKeyEncoder(cubeSegment, Cuboid.findById(cubeInstance.getDescriptor(), getCuboididFromDiv(div)));
                return true;
            }
            return false;
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
            return ((float) divIndex) / divs.size();
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
