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

package io.kyligence.kap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.query.KylinTestBase;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetConfig;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

public class ITKapParquetTest extends KylinTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ITKapParquetTest.class);

    private static String fileName = "/tmp/kylin/" + RandomStringUtils.randomAlphanumeric(8) + ".parquet";
    private static Path path;
    private static Configuration conf = new Configuration();
    private MessageType type;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        path = new Path(fileName);
        logger.info("origin path: " + path);
        conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
        FileSystem fs = HadoopUtil.getFileSystem(path, conf);
        path = fs.makeQualified(path);
        logger.info("qualified path: " + path);
        fs.deleteOnExit(path);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem(path, conf);
        fs.deleteOnExit(path);
    }

    @Test
    public void testParquetReadWrite() throws Exception {
         type = new MessageType("test",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "key"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m2"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m3"));
         writeRows(ParquetConfig.PagesPerGroup * ParquetConfig.RowsPerPage, 10);
         readRows();
    }

    protected void writeRows(int rowsPerGroup, int groupCnt) throws Exception {
        List<ByteArray> randomBytes = randomBytes(rowsPerGroup, type);

        long t = System.currentTimeMillis();

        int keyLen = type.getColumns().get(0).getTypeLength();
        int[] valueLen = new int[type.getColumns().size() - 1];
        for (int i = 1; i < type.getColumns().size(); i++) {
            valueLen[i - 1] = type.getColumns().get(i).getTypeLength();
        }

        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(new Path(fileName))
                .setType(type).build();
        for (int g = 0; g < groupCnt; g++) {
            for (int i = 0; i < rowsPerGroup; i++) {
                byte[] bytes = randomBytes.get(i).array();
                writer.writeRow(bytes, 0, keyLen, bytes, valueLen);
            }
        }

        t = System.currentTimeMillis() - t;
        System.out.println("Writen " + (rowsPerGroup * groupCnt) + " rows in " + ((float) t / 1000) + " seconds, speed "
                + ((float) rowsPerGroup * groupCnt / 1000 / t) + " million rows/s");
        writer.close();
    }

    private List<ByteArray> randomBytes(int rowCnt, MessageType type) {
        int len = 0;
        for (ColumnDescriptor col : type.getColumns()) {
            len += col.getTypeLength();
        }

        List<ByteArray> arrList = new ArrayList<>(rowCnt);
        Random rand = new Random();
        for (int i = 0; i < rowCnt; i++) {
            ByteArray arr = new ByteArray(len);
            rand.nextBytes(arr.array());
            arrList.add(arr);
        }
        Collections.sort(arrList);

        return arrList;
    }


    private void readRows() throws IOException {
        long t = System.currentTimeMillis();
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(new Path(fileName)).setConf(new Configuration())
                .build();

        long t2 = System.currentTimeMillis();
        System.out.println("Read footer takes " + (t2 - t) + " ms");

        long cnt = 0;
        while (bundleReader.read() != null) {
            cnt++;
            continue;
        }

        long t3 = System.currentTimeMillis();
        System.out.println("Read " + cnt + " rows, take " + (t3 - t2) + " ms");

        bundleReader.close();

        t = System.currentTimeMillis() - t;
        System.out.println("Read " + cnt + " rows in " + ((float) t / 1000) + " seconds, speed "
                + ((float) cnt / 1000 / t) + " million rows/s");
    }
}
