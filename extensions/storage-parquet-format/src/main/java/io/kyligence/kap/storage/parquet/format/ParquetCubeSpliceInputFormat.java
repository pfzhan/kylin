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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceReader;

public class ParquetCubeSpliceInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
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

        private HBaseColumnFamilyDesc[] cfDescs;
        private MeasureCodec measureCodec;

        private ParquetSpliceReader spliceReader = null;
        private ParquetBundleReader reader = null;
        private List<String> divs;
        private int divIndex = 0;

        private Text key = new Text();
        private Text val = new Text();

        // Default constructor to be used normally. 
        public ParquetCubeSpliceReader() {

        }

        // This constructor is only used for testing. 
        protected ParquetCubeSpliceReader(Configuration conf, Path path, CubeSegment cubeSegment) throws IOException {

            this.conf = conf;
            this.path = path;
            this.cubeSegment = cubeSegment;

            this.cfDescs = cubeSegment.getCubeDesc().getHbaseMapping().getColumnFamily();
            List<MeasureDesc> cfMeasures = Lists.newArrayList();
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    MeasureDesc[] measures = colDesc.getMeasures();
                    cfMeasures.addAll(new ArrayList<MeasureDesc>(Arrays.asList(measures)));
                }
            }
            this.measureCodec = new MeasureCodec(cfMeasures);

            this.spliceReader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path).build();

            divs = Lists.newArrayList();

            divs.add(new String("mockedDiv"));
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();
            this.path = fileSplit.getPath();
            this.kylinConfig = KylinConfig.loadKylinPropsAndMetadata();

            String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
            String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
            String[] requiredCuboids;

            logger.info("cubeName is " + cubeName + " and segmentID is " + segmentID);

            this.cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            this.cubeSegment = cubeInstance.getSegmentById(segmentID);
            // init with first shard file
            this.spliceReader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path).build();

            this.cfDescs = cubeSegment.getCubeDesc().getHbaseMapping().getColumnFamily();
            List<MeasureDesc> cfMeasures = Lists.newArrayList();
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    MeasureDesc[] measures = colDesc.getMeasures();
                    cfMeasures.addAll(new ArrayList<MeasureDesc>(Arrays.asList(measures)));
                }
            }
            this.measureCodec = new MeasureCodec(cfMeasures);

            if (context.getConfiguration().get(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS).equals("All")) {
                // "All" for all divs
                requiredCuboids = getAllCuboidsFromFile();
            } else {
                // <cuboid-id1>,<cuboid-id2>,...,<cuboid-idn>
                requiredCuboids = context.getConfiguration().get(ParquetFormatConstants.KYLIN_REQUIRED_CUBOIDS)
                        .split(",");
            }

            filterDivs(requiredCuboids);
        }

        private String[] getAllCuboidsFromFile() {
            return Lists.newArrayList(getCuboid2DivMap().keySet()).toArray(new String[0]);
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

        // This function is only used for testing. 
        protected void nextValueWithoutKey() throws IOException {
            List<Object> data = reader.read();
            setVal(data);
        }

        private boolean getNextReader() throws IOException {
            if (divIndex < divs.size()) {
                if (reader != null) {
                    reader.close();
                }
                String div = divs.get(divIndex++);
                reader = spliceReader.getDivReader(div);
                rowKeyEncoder = new RowKeyEncoder(cubeSegment,
                        Cuboid.findById(cubeInstance, getCuboididFromDiv(div)));
                return true;
            }
            return false;
        }

        // This function is only used for testing. 
        protected boolean setInternalReader(ParquetBundleReader reader) {
            this.reader = reader;
            if (this.reader != null) {
                return true;
            }
            return false;
        }

        private void setVal(List<Object> data) {

            // Step 1: transform data object list to byte array. 
            int cfValueBytesLength = 0;
            for (int i = 1; i < data.size(); ++i) {
                cfValueBytesLength += ((Binary) data.get(i)).getBytes().length;
            }
            byte[] cfValueBytes = new byte[cfValueBytesLength];
            for (int i = 1, cfIndex = 0; i < data.size(); ++i) {
                byte[] src = ((Binary) data.get(i)).getBytes();
                System.arraycopy(src, 0, cfValueBytes, cfIndex, src.length);
                cfIndex += src.length;
            }

            // Step 2: calculate byte array length for measures as the order they were defined.  
            int[] valueLength = measureCodec.getPeekLength(ByteBuffer.wrap(cfValueBytes));
            int[] valueLengthInMeasureOrder = new int[valueLength.length];
            int idx = 0;
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    int[] measureIndexes = colDesc.getMeasureIndex();
                    for (int measureIndex : measureIndexes) {
                        valueLengthInMeasureOrder[measureIndex] = valueLength[idx];
                        idx++;
                    }
                }
            }

            // Step 3: calculate value offsets in result byte array to which measures will be copied. 
            int[] valueOffsets = new int[valueLength.length];
            for (int i = 0, valueOffset = 0; i < valueOffsets.length; i++) {
                valueOffsets[i] = valueOffset;
                valueOffset += valueLengthInMeasureOrder[i];
            }

            // Step 4: copy array bytes as measure order. 
            byte[] valueBytes = new byte[cfValueBytes.length];
            int cfValueOffset = 0;
            for (HBaseColumnFamilyDesc cfDesc : cfDescs) {
                HBaseColumnDesc[] colDescs = cfDesc.getColumns();
                for (HBaseColumnDesc colDesc : colDescs) {
                    int[] measureIndexes = colDesc.getMeasureIndex();
                    for (int measureIndex : measureIndexes) {
                        System.arraycopy(cfValueBytes, cfValueOffset, valueBytes, valueOffsets[measureIndex],
                                valueLengthInMeasureOrder[measureIndex]);
                        cfValueOffset += valueLengthInMeasureOrder[measureIndex];
                    }
                }
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
