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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.storage.parquet.format.ParquetCubeSpliceInputFormat.ParquetCubeSpliceReader;
import io.kyligence.kap.storage.parquet.format.ParquetCubeSpliceOutputFormat.ParquetCubeSpliceWriter;
import io.kyligence.kap.storage.parquet.format.file.AbstractParquetFormatTest;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;

public class ParquetWriteAndReadColumnFamilyTest extends AbstractParquetFormatTest {

    protected List<MeasureDesc> mockedMeasures;
    protected HBaseMappingDesc mockedHBaseMapping;

    protected final int f1Int = -1;
    protected final int f2Int = 1024;
    protected final long f1Long = -1L;
    protected final long f2Long = 2147483648L;
    protected final double f1Double = 0.0;
    protected final double f2Double = 4.0;

    public ParquetWriteAndReadColumnFamilyTest() throws IOException {
        super();
        initMeasures();
        initHBaseMapping();
        initMeasureReferenceToColumnFamily();
    }

    @Override
    public void setup() throws IOException {
        super.setup();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // default compression code is SNAPPY, not supported on mac, so disable it
        kylinConfig.setProperty("kap.storage.columnar.page-compression", "");
    }

    protected void initMeasures() {
        mockedMeasures = new ArrayList<MeasureDesc>();
        for (int i = 0; i < 6; i++) {
            MeasureDesc measure = new MeasureDesc();
            measure.setName("MEASURE" + i);
            FunctionDesc function = null;
            switch (i) {
            case 0:
            case 1:
                function = FunctionDesc.newInstance(null, null, "int4");
                break;
            case 2:
            case 3:
                function = FunctionDesc.newInstance(null, null, "long8");
                break;
            case 4:
            case 5:
                function = FunctionDesc.newInstance(null, null, "double");
                break;
            default:
            }
            measure.setFunction(function);
            mockedMeasures.add(measure);
        }
    }

    protected void initHBaseMapping() {
        mockedHBaseMapping = new HBaseMappingDesc();

        HBaseColumnFamilyDesc[] columnFamily = new HBaseColumnFamilyDesc[2];

        for (int i = 0; i < 2; i++) {
            HBaseColumnFamilyDesc cf = new HBaseColumnFamilyDesc();
            HBaseColumnDesc col = new HBaseColumnDesc();
            List<String> measureRefsList = new ArrayList<String>();
            for (int j = 0; j < 3; j++) {
                String measureRef = mockedMeasures.get(2 * j + i).getName();
                measureRefsList.add(measureRef);
            }
            String[] measureRefs = new String[3];
            for (int j = 0; j < 3; j++) {
                measureRefs[j] = measureRefsList.get(j);
            }
            col.setMeasureRefs(measureRefs);
            col.setQualifier("M");
            cf.setColumns(new HBaseColumnDesc[] { col });
            cf.setName("F" + (i + 1));
            columnFamily[i] = cf;
        }

        mockedHBaseMapping.setColumnFamily(columnFamily);
    }

    protected void initMeasureReferenceToColumnFamily() {

        Map<String, MeasureDesc> measureLookup = new HashMap<String, MeasureDesc>();
        for (MeasureDesc m : mockedMeasures)
            measureLookup.put(m.getName(), m);
        Map<String, Integer> measureIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < mockedMeasures.size(); i++)
            measureIndexLookup.put(mockedMeasures.get(i).getName(), i);

        BitSet checkEachMeasureExist = new BitSet();
        for (HBaseColumnFamilyDesc cf : mockedHBaseMapping.getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                String[] colMeasureRefs = c.getMeasureRefs();
                MeasureDesc[] measureDescs = new MeasureDesc[colMeasureRefs.length];
                int[] measureIndex = new int[colMeasureRefs.length];
                for (int i = 0; i < colMeasureRefs.length; i++) {
                    measureDescs[i] = measureLookup.get(colMeasureRefs[i]);
                    measureIndex[i] = measureIndexLookup.get(colMeasureRefs[i]);
                    checkEachMeasureExist.set(measureIndex[i]);
                }
                c.setMeasures(measureDescs);
                c.setMeasureIndex(measureIndex);
                c.setColumnFamilyName(cf.getName());
            }
        }
    }

    @SuppressWarnings("serial")
    protected class MockedCubeSegment extends CubeSegment {

        @Override
        public String getName() {
            return "mockedCubeSegment";
        }

        @Override
        public CubeDesc getCubeDesc() {
            return new MockedCubeDesc();
        }

    }

    @SuppressWarnings("serial")
    protected class MockedCubeDesc extends CubeDesc {

        @Override
        public List<MeasureDesc> getMeasures() {
            return mockedMeasures;
        }

        @Override
        public HBaseMappingDesc getHbaseMapping() {
            return mockedHBaseMapping;
        }

    }

    @SuppressWarnings("serial")
    protected class MockedRowKeyEncoder extends RowKeyEncoder {

        public MockedRowKeyEncoder(CubeSegment cubeSeg, Cuboid cuboid) {
            super(cubeSeg, cuboid);
        }

    }

    protected Text mockKey() {
        byte[] keyBytes = new byte[RowConstants.ROWKEY_SHARDID_LEN + RowConstants.ROWKEY_CUBOIDID_LEN + 4];
        short shardId = -1;
        long cuboidId = -1L;
        int keyBody = 2017;
        byte[] shardIdBytes = Bytes.toBytes(shardId);
        byte[] cuboidIdBytes = Bytes.toBytes(cuboidId);
        byte[] keyBodyBytes = Bytes.toBytes(keyBody);
        System.arraycopy(shardIdBytes, 0, keyBytes, 0, RowConstants.ROWKEY_SHARDID_LEN);
        System.arraycopy(cuboidIdBytes, 0, keyBytes, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
        System.arraycopy(keyBodyBytes, 0, keyBytes, RowConstants.ROWKEY_SHARDID_LEN + RowConstants.ROWKEY_CUBOIDID_LEN,
                4);
        return new Text(keyBytes);
    }

    protected Text mockValue() {

        Map<String, Object> measureValueMap = new HashMap<String, Object>();
        measureValueMap.put("MEASURE0", f1Int);
        measureValueMap.put("MEASURE1", f2Int);
        measureValueMap.put("MEASURE2", f1Long);
        measureValueMap.put("MEASURE3", f2Long);
        measureValueMap.put("MEASURE4", f1Double);
        measureValueMap.put("MEASURE5", f2Double);

        ByteBuffer out = ByteBuffer.allocate(200);

        for (MeasureDesc measure : mockedMeasures) {
            DataTypeSerializer serializer = DataTypeSerializer.create(measure.getFunction().getReturnDataType());
            serializer.serialize(measureValueMap.get(measure.getName()), out);
        }

        return new Text(out.array());
    }

    @Test
    public void testWriteAndReadColumnFamily() throws Exception {

        cleanTestFile(path);

        Configuration conf = new Configuration();
        CubeSegment cubeSegment = new MockedCubeSegment();

        ParquetCubeSpliceWriter writer = new ParquetCubeSpliceWriter(conf, path, cubeSegment);

        Text key = mockKey();
        Text value = mockValue();

        writer.write(key, value);

        writer.close(null);

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FileStatus[] files = fs.listStatus(path);
        Assert.assertEquals(files.length, 1);
        Path readPath = null;
        for (FileStatus file : files) {
            readPath = file.getPath();
        }

        ParquetCubeSpliceReader reader = new ParquetCubeSpliceReader(conf, readPath, cubeSegment);

        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(readPath).setConf(conf).build();

        reader.setInternalReader(bundleReader);

        reader.nextValueWithoutKey();

        Text result = reader.getCurrentValue();
        byte[] resultBytes = result.getBytes();

        ByteBuffer in = ByteBuffer.wrap(resultBytes);

        Object[] resultObjects = new Object[6];

        int idx = 0;
        for (MeasureDesc measure : mockedMeasures) {
            DataTypeSerializer serializer = DataTypeSerializer.create(measure.getFunction().getReturnDataType());
            resultObjects[idx] = serializer.deserialize(in);
            idx++;
        }

        Assert.assertEquals((Integer) resultObjects[0], Integer.valueOf(f1Int));
        Assert.assertEquals((Integer) resultObjects[1], Integer.valueOf(f2Int));
        Assert.assertEquals((Long) resultObjects[2], Long.valueOf(f1Long));
        Assert.assertEquals((Long) resultObjects[3], Long.valueOf(f2Long));
        Assert.assertEquals((Double) resultObjects[4], Double.valueOf(f1Double));
        Assert.assertEquals((Double) resultObjects[5], Double.valueOf(f2Double));

        reader.close();
    }
}
