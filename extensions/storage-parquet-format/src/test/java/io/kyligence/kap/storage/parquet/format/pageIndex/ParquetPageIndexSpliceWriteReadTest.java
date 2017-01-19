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

package io.kyligence.kap.storage.parquet.format.pageIndex;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class ParquetPageIndexSpliceWriteReadTest extends LocalFileMetadataTestCase {
    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @Test
    public void testWrite() throws IOException, ClassNotFoundException {
        // arrange
        int dataSize = 100;
        int totalPageNum = 150;
        int divSize = 10;

        int maxVal1 = 50;
        int maxVal2 = 100;
        int cardinality1 = 50;
        int cardinality2 = 100;

        int[] columnLength = { (Integer.SIZE - Integer.numberOfLeadingZeros(maxVal1) + 7) / 8, (Integer.SIZE - Integer.numberOfLeadingZeros(maxVal2) + 7) / 8 };
        int[] cardinality = { cardinality1, cardinality2 };
        String[] columnName = { "1", "2" };
        int[] data1 = new int[dataSize];
        int[] data2 = new int[dataSize];
        Map<String, Map<Integer, Set<Integer>>> divMap1 = Maps.newLinkedHashMap();
        Map<String, Map<Integer, Set<Integer>>> divMap2 = Maps.newLinkedHashMap();
        Random random = new Random();
        for (int i = 0; i < dataSize; i++) {
            data1[i] = random.nextInt(maxVal1);
            data2[i] = random.nextInt(maxVal2);
        }
        Set<Integer> s1 = Sets.newHashSet();
        for (int a : data1) {
            s1.add(a);
        }
        Set<Integer> s2 = Sets.newHashSet();
        for (int a : data2) {
            s2.add(a);
        }
        File indexFile = File.createTempFile("local", "inv");
        System.out.println("Temp index file: " + indexFile);

        // write
        boolean[] onlyEq = { false, false };
        FSDataOutputStream outputStream = new FSDataOutputStream(new FileOutputStream(indexFile));
        // append header
        outputStream.writeLong(0);
        ParquetPageIndexSpliceWriter spliceWriter = new ParquetPageIndexSpliceWriter(outputStream);
        for (int i = 0; i < dataSize; i++) {
            int div = i / divSize;
            if ((i % divSize) == 0) {
                divMap1.put(String.valueOf(div), Maps.<Integer, Set<Integer>> newLinkedHashMap());
                divMap2.put(String.valueOf(div), Maps.<Integer, Set<Integer>> newLinkedHashMap());
                if (spliceWriter.isDivStarted()) {
                    spliceWriter.endDiv();
                }
                spliceWriter.startDiv(String.valueOf(div), columnName, columnLength, cardinality, onlyEq);
            }
            byte[] buffer1 = new byte[columnLength[0]];
            byte[] buffer2 = new byte[columnLength[1]];
            int pageId = random.nextInt(totalPageNum);
            BytesUtil.writeUnsigned(data1[i], buffer1, 0, columnLength[0]);
            BytesUtil.writeUnsigned(data2[i], buffer2, 0, columnLength[1]);
            spliceWriter.write(Lists.newArrayList(buffer1, buffer2), pageId);

            if (!divMap1.get(String.valueOf(div)).containsKey(data1[i])) {
                divMap1.get(String.valueOf(div)).put(data1[i], Sets.<Integer> newLinkedHashSet());
            }
            divMap1.get(String.valueOf(div)).get(data1[i]).add(pageId);
            if (!divMap2.get(String.valueOf(div)).containsKey(data2[i])) {
                divMap2.get(String.valueOf(div)).put(data2[i], Sets.<Integer> newLinkedHashSet());
            }
            divMap2.get(String.valueOf(div)).get(data2[i]).add(pageId);
        }
        ObjectOutputStream oos = spliceWriter.closeWithoutStream();
        // append a tail
        outputStream.writeLong(0);
        oos.close();

        // read
        FSDataInputStream inputStream = HadoopUtil.getWorkingFileSystem().open(new Path(indexFile.getAbsolutePath()));
        long fileSize = HadoopUtil.getWorkingFileSystem().getFileStatus(new Path(indexFile.getAbsolutePath())).getLen();
        ParquetPageIndexSpliceReader spliceReader = new ParquetPageIndexSpliceReader(inputStream, fileSize - 8, 8);

        for (String div : divMap1.keySet()) {
            ParquetPageIndexReader reader = spliceReader.getIndexReader(div);
            Map<Integer, Set<Integer>> d1 = divMap1.get(div);
            for (Integer k : d1.keySet()) {
                byte[] buffer = new byte[columnLength[0]];
                Set<Integer> expected = d1.get(k);

                BytesUtil.writeUnsigned(k, buffer, 0, columnLength[0]);
                Set<Integer> actual = Sets.newHashSet(reader.readColumnIndex(0).getRows(new ByteArray(buffer)));
                assertEquals(expected, actual);
            }

            Map<Integer, Set<Integer>> d2 = divMap2.get(div);
            for (Integer k : d2.keySet()) {
                byte[] buffer = new byte[columnLength[1]];
                Set<Integer> expected = d2.get(k);

                BytesUtil.writeUnsigned(k, buffer, 0, columnLength[1]);
                Set<Integer> actual = Sets.newHashSet(reader.readColumnIndex(1).getRows(new ByteArray(buffer)));
                assertEquals(expected, actual);
            }
            System.out.println(reader.getPageTotalNum(0));
            System.out.println(reader.getPageTotalNum(1));
            reader.closeWithoutStream();
        }
        inputStream.close();
    }
}
