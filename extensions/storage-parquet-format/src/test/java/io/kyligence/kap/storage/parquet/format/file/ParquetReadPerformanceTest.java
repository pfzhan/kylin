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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Performance test of reading parquet and loading data into application memory.
 * 
 * Write performance:
 * - Writen 10,000,000 rows in 20.473 seconds, speed 0.4884482 M/s
 * - Writen 10,000,000 rows in 12.249 seconds, speed 0.8163932 M/s
 * - Writen 10,000,000 rows in 12.373 seconds, speed 0.80821145 M/s
 * 
 * testParquetBundleReaderPerformance:
 * - Writen 10000000 rows in 19.18 seconds, speed 0.52137643 million rows/s
 * - Read footer takes 320 ms
 * - Read 10000000 rows, take 6284 ms
 * - Read 10000000 rows in 6.604 seconds, speed 1.5142338 million rows/s
 * - Read footer takes 37 ms
 * - Read 10000000 rows, take 5690 ms
 * - Read 10000000 rows in 5.783 seconds, speed 1.7292063 million rows/s
 * - Read footer takes 31 ms
 * - Read 10000000 rows, take 5356 ms
 * - Read 10000000 rows in 5.406 seconds, speed 1.8497965 million rows/s
 *
 * testDremioParquetRowiseReaderPerformance:
 * - Writen 10000000 rows in 18.824 seconds, speed 0.5312367 million rows/s
 * - Read footer takes 246 ms
 * - Read 10000000 rows, take 7335 ms
 * - Read 10000000 rows in 7.581 seconds, speed 1.3190871 million rows/s
 * - Read footer takes 49 ms
 * - Read 10000000 rows, take 6804 ms
 * - Read 10000000 rows in 6.853 seconds, speed 1.4592149 million rows/s
 * - Read footer takes 36 ms
 * - Read 10000000 rows, take 6783 ms
 * - Read 10000000 rows in 6.82 seconds, speed 1.4662757 million rows/s
 */

@Ignore("Performance test don't run by default")
public class ParquetReadPerformanceTest extends AbstractParquetFormatTest {
    public ParquetReadPerformanceTest() throws IOException {
        super();
        type = new MessageType("test",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "key"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m2"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m3"));
    }

    protected void writeRows(int rowsPerGroup, int groupCnt) throws Exception {
        List<ByteArray> randomBytes = randomBytes(rowsPerGroup, type);

        long t = System.currentTimeMillis();

        int keyLen = type.getColumns().get(0).getTypeLength();
        int[] valueLen = new int[type.getColumns().size() - 1];
        for (int i = 1; i < type.getColumns().size(); i++) {
            valueLen[i - 1] = type.getColumns().get(i).getTypeLength();
        }

        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path)
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

    @Test
    public void testParquetBundleReaderPerformance() throws Exception {
        writeRows(groupSize, 10);
        //        cleanUpFile();
        //        writeRows(groupSize, 10);
        //        cleanUpFile();
        //        writeRows(groupSize, 10);

        readRows();
        readRows();
        readRows();
    }

    private void readRows() throws IOException {
        long t = System.currentTimeMillis();
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(new Configuration())
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

    /**
     * This is a mimic of ParquetRowiseReader, and tests its performance.
     */
    @Test
    public void testDremioParquetRowiseReaderPerformance() throws Exception {
        writeRows(groupSize, 10);
        //        cleanUpFile();
        //        writeRows(groupSize, 10);
        //        cleanUpFile();
        //        writeRows(groupSize, 10);

        readRows2();
        readRows2();
        readRows2();
    }

    @SuppressWarnings("deprecation")
    private void readRows2() throws IOException {
        long t = System.currentTimeMillis();

        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), path);
        ParquetFileReader reader = new ParquetFileReader(new Configuration(), path, footer);
        ColumnIOFactory factory = new ColumnIOFactory(false);
        MessageColumnIO columnIO = factory.getColumnIO(type);

        long t2 = System.currentTimeMillis();
        System.out.println("Read footer takes " + (t2 - t) + " ms");

        PageReadStore rowGroup;
        long cnt = 0;
        while ((rowGroup = reader.readNextRowGroup()) != null) {
            RecordReader<Void> recordReader = columnIO.getRecordReader(rowGroup, new RecordMaterializer<Void>() {

                @Override
                public Void getCurrentRecord() {
                    return null;
                }

                @Override
                public GroupConverter getRootConverter() {
                    return new GroupConverter() {

                        @Override
                        public void end() {
                        }

                        @Override
                        public Converter getConverter(int fieldIndex) {
                            return new PrimitiveConverter() {

                                byte[] store = new byte[200];

                                @Override
                                public void addBinary(Binary value) {
                                    ByteBuffer buf = value.toByteBuffer();
                                    int remaining = buf.remaining();
                                    buf.get(store, 0, remaining);
                                }

                                @Override
                                public PrimitiveConverter asPrimitiveConverter() {
                                    return this;
                                }

                                @Override
                                public boolean isPrimitive() {
                                    return true;
                                }
                            };
                        }

                        @Override
                        public void start() {
                        }
                    };
                }
            });

            long nRows = rowGroup.getRowCount();
            for (int i = 0; i < nRows; i++) {
                recordReader.read();
                cnt++;
            }
        }

        long t3 = System.currentTimeMillis();
        System.out.println("Read " + cnt + " rows, take " + (t3 - t2) + " ms");

        reader.close();

        t = System.currentTimeMillis() - t;
        System.out.println("Read " + cnt + " rows in " + ((float) t / 1000) + " seconds, speed "
                + ((float) cnt / 1000 / t) + " million rows/s");
    }

    private void cleanUpFile() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

}
