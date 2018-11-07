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
package io.kyligence.kap.engine.spark.storage;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.HDFSResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Files;

import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;

@SuppressWarnings("serial")
public class NParquetStorageTest extends NLocalWithSparkSessionTest implements Serializable {

    @Test
    public void testRoundTripWithoutCF() throws IOException {
        testRoundTripForCuboidLayout(2L, new ParquetStorage());
        testRoundTripForCuboidLayout(2L, new NParquetStorage());
    }

    @Test
    public void testRoundTripWithCF() throws IOException {
        testRoundTripForCuboidLayout(1L,  new ParquetStorage());
        testRoundTripForCuboidLayout(1L,  new NParquetStorage());
    }

    private void testRoundTripForCuboidLayout(long layoutId, NSparkCubingEngine.NSparkCubingStorage storage) throws IOException {
        final String dataJson = "0,1,2,1000\n0,1,2,1\n3,4,5,10000001";

        File dataFile = File.createTempFile("tmp", ".csv");
        FileUtils.writeStringToFile(dataFile, dataJson, Charset.defaultCharset());
        final String dfName = "all_fixed_length";

        Dataset<Row> dataflow = ss.read().option("header", "true").csv(dataFile.getAbsolutePath()).repartition(2);
        List<Row> expected = dataflow.collectAsList();
        dataflow.show();

        File tmpDir = new File(Files.createTempDir(), UUID.randomUUID().toString());
        File hdfsDir = new File(Files.createTempDir(), UUID.randomUUID().toString());

        KylinConfig config = getTestConfig();
        Map<String, String> params = new HashMap<>();
        params.put("path", tmpDir.getAbsolutePath());
        StorageURL newStorageUrl = new StorageURL(config.getMetadataUrlPrefix(), HDFSResourceStore.HDFS_SCHEME, params);
        KylinConfig hdfsConfig = KylinConfig.createKylinConfig(config);
        hdfsConfig.setProperty("kylin.metadata.url", newStorageUrl.toString());
        hdfsConfig.setProperty("kylin.env.hdfs-working-dir", hdfsDir.getAbsolutePath());
        ResourceTool.copy(config, hdfsConfig);
        File tmpProps = File.createTempFile("kylin", ".properties");
        hdfsConfig.exportToFile(tmpProps);
        HadoopUtil.getWorkingFileSystem().copyToLocalFile(true, new Path(tmpProps.getAbsolutePath()),
                new Path(tmpDir.getAbsolutePath(), "kylin.properties"));

        NDataflow df = NDataflowManager.getInstance(hdfsConfig, "default").getDataflow(dfName);
        NDataSegDetails segCuboids = df.getLastSegment().getSegDetails();
        final NCuboidLayout layout = df.getCubePlan().getCuboidLayout(layoutId);
        final int dimNum = layout.getOrderedDimensions().size();

        NDataCuboid cuboidInstance = NDataCuboid.newDataCuboid(segCuboids, layoutId);

        StructType schema = new StructType();
        String[] tableHead = { "0", "1", "2", "1000" };
        for (int i = 0; i < 4; i++) {
            schema = schema.add(tableHead[i], DataTypes.BinaryType, false);
        }
        final List<TblColRef> dimColRefs = layout.getColumns();
        final NCubeDimEncMap dimEncMap = new NCubeDimEncMap(segCuboids.getDataSegment());
        final MeasureCodec measureCodec = new MeasureCodec(
                layout.getOrderedMeasures().values().toArray(new MeasureDesc[0]));
        Dataset<Row> toBytes = dataflow.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                Object[] ret = new Object[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    if (dimNum > i) {
                        DimensionEncoding dimEnc = dimEncMap.get(dimColRefs.get(i));
                        byte[] buf = new byte[dimEnc.getLengthOfEncoding()];
                        dimEnc.encode(value.getString(i), buf, 0);
                        ret[i] = buf;
                    } else {
                        ByteBuffer buf = ByteBuffer.wrap(new byte[256]);
                        try {
                            measureCodec.encode(i - dimNum, Long.parseLong(value.getString(i)), buf);
                        }catch (Exception e){
                            System.out.println();
                        }
                        ret[i] = new ByteArray(buf.array(), 0, buf.position()).toBytes();
                    }
                }
                return RowFactory.create(ret);
            }
        }, RowEncoder.apply(schema));

        storage.saveCuboidData(cuboidInstance, toBytes, ss);

        dataflow = storage.getCuboidData(cuboidInstance, ss);

        schema = new StructType();
        for (int i = 0; i < 4; i++) {
            schema = schema.add(tableHead[i], DataTypes.StringType, false);
        }
        Dataset<Row> toStrings = dataflow.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                String[] ret = new String[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    if (dimNum > i) {
                        DimensionEncoding dimEnc = dimEncMap.get(dimColRefs.get(i));
                        ret[i] = dimEnc.decode((byte[]) value.get(i), 0, dimEnc.getLengthOfEncoding());
                    } else {
                        ByteBuffer buf = ByteBuffer.wrap((byte[]) value.get(i));
                        ret[i] = String.valueOf(measureCodec.decode(i - dimNum, buf));
                    }
                }
                return RowFactory.create(ret);
            }
        }, RowEncoder.apply(schema));

        List<Row> actual = toStrings.collectAsList();
        toStrings.show();

        System.out.println(expected);
        System.out.println(actual);
        Assert.assertEquals(expected.size(), actual.size());
        Assert.assertTrue(expected.containsAll(actual));
        dataFile.delete();
    }
}