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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Ignore;
import org.junit.Test;

public class ParquetBundleReaderPerformanceTest extends AbstractParquetFormatTest {
    public ParquetBundleReaderPerformanceTest() throws IOException {
        super();
        type = new MessageType("test", new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 26, "key"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 16, "m1"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 16, "m2"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 16, "m3"));
    }

    protected void writeRows(int rowCnt) throws Exception {
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path).setType(type).build();
        byte[] key = "abcdefghijklmnopqrstuvwxyz".getBytes();
        byte[] m = "aaaabbbbccccddddaaaabbbbccccddddaaaabbbbccccdddd".getBytes();
        long t = System.currentTimeMillis();
        for (int i = 0; i < rowCnt; ++i) {
            writer.writeRow(key, 0, 26, m, new int[] { 16, 16, 16 });
        }
        t = System.currentTimeMillis() - t;
        System.out.println("Write file speed " + ((float) rowCnt / 1000 / t) + "M/s");
        writer.close();
    }

    @Test
    @Ignore
    public void testReadInBundleCache() throws Exception {
        writeRows(groupSize);

        long t = System.currentTimeMillis();
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(new Path("/Users/roger/0.parquet")).setConf(new Configuration()).build();

        long t2 = System.currentTimeMillis() - t;
        System.out.println("Create takes " + t2 + " ms");

        while(bundleReader.read() != null)
            continue;

        t = System.currentTimeMillis() - t;
        System.out.println("Take " + t + " ms");

        bundleReader.close();
    }

    @Test
    @Ignore
    public void testReadInBundle() throws Exception {
        writeRows(groupSize);

        long t = System.currentTimeMillis();
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(new Path("/Users/roger/0.parquet")).setConf(new Configuration()).build();

        long t2 = System.currentTimeMillis() - t;
        System.out.println("Create takes " + t2 + " ms");

        while(bundleReader.read() != null)
            continue;

        t = System.currentTimeMillis() - t;
        System.out.println("Take " + t + " ms");
        bundleReader.close();
    }

    @Test
    @Ignore
    public void testReadInBundleColumn() throws Exception {
        writeRows(groupSize);

        long t = System.currentTimeMillis();
        List<ParquetColumnReader> readerList = new ArrayList<>();
        Path p = new Path("/Users/roger/0.parquet");
        Configuration c = new Configuration();
        for (int i = 0; i < 300; i++) {
            ParquetRawReader rawReader = new ParquetRawReader(c, p, null, null, 0);
            ParquetColumnReader columnReader = new ParquetColumnReader(rawReader, i, null);
            readerList.add(columnReader);
        }

        long t2 = System.currentTimeMillis() - t;
        System.out.println("Create takes " + t2 + " ms");

        for (int i = 0; i < 300; i++) {
            ParquetColumnReader reader = readerList.get(i);
            GeneralValuesReader gReader;
            while ((gReader = reader.getNextValuesReader()) != null) {
                while (gReader.readData() != null)
                    continue;
            }
        }
        t = System.currentTimeMillis() - t;
        System.out.println("Take " + t + " ms");
    }
}
