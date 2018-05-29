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

package io.kyligence.kap.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetConfig;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

/**
 * 
--------------------------------------------------------
Benchmark columns=1, rowSizeBytes=200, rowsPerGroup=1000000
Test file: file:/tmp/StoragePerformanceBenchmark.kap

==== Test write performance ====
Writen 10000000 rows in 8.52 seconds, speed 1.1737089 million rows/s

==== Test Read By Rows ====
Read 10000000 rows in 1.771 seconds, speed 5.6465273 million rows/s
Read 10000000 rows in 1.648 seconds, speed 6.067961 million rows/s
Read 10000000 rows in 1.657 seconds, speed 6.035003 million rows/s

==== Test Read By Pages ====
...footerReadCnt    1
...footerReadTime   0
...pageReadOverallPageCnt   1000
...pageReadOverallCellCnt   10000000
...pageReadOverallTime  1329
...pageReadHeaderCnt    1000
...pageReadHeaderTime   0
...pageReadIOAndDecompressRawIOBytes    2000402000
...pageReadIOAndDecompressBytes 2000402000
...pageReadIOAndDecompressTime  1328
...pageReadDecodeBytes  2000402000
...pageReadDecodeTime   1
Read 10000000 rows in 1.476 seconds, speed 6.775068 million rows/s
...footerReadCnt    1
...footerReadTime   0
...pageReadOverallPageCnt   1000
...pageReadOverallCellCnt   10000000
...pageReadOverallTime  1491
...pageReadHeaderCnt    1000
...pageReadHeaderTime   0
...pageReadIOAndDecompressRawIOBytes    2000402000
...pageReadIOAndDecompressBytes 2000402000
...pageReadIOAndDecompressTime  1422
...pageReadDecodeBytes  2000402000
...pageReadDecodeTime   54
Read 10000000 rows in 1.564 seconds, speed 6.393862 million rows/s
...footerReadCnt    1
...footerReadTime   0
...pageReadOverallPageCnt   1000
...pageReadOverallCellCnt   10000000
...pageReadOverallTime  1362
...pageReadHeaderCnt    1000
...pageReadHeaderTime   16
...pageReadIOAndDecompressRawIOBytes    2000402000
...pageReadIOAndDecompressBytes 2000402000
...pageReadIOAndDecompressTime  1267
...pageReadDecodeBytes  2000402000
...pageReadDecodeTime   79
Read 10000000 rows in 1.409 seconds, speed 7.097232 million rows/s

--------------------------------------------------------
Benchmark columns=4, rowSizeBytes=200, rowsPerGroup=1000000
Test file: file:/tmp/StoragePerformanceBenchmark.kap

==== Test write performance ====
Writen 10000000 rows in 9.05 seconds, speed 1.1049724 million rows/s

==== Test Read By Rows ====
Read 10000000 rows in 2.281 seconds, speed 4.3840423 million rows/s
Read 10000000 rows in 2.137 seconds, speed 4.679457 million rows/s
Read 10000000 rows in 2.119 seconds, speed 4.7192073 million rows/s

==== Test Read By Pages ====
...footerReadCnt    1
...footerReadTime   0
...pageReadOverallPageCnt   4000
...pageReadOverallCellCnt   40000000
...pageReadOverallTime  1355
...pageReadHeaderCnt    4000
...pageReadHeaderTime   43
...pageReadIOAndDecompressRawIOBytes    2001604000
...pageReadIOAndDecompressBytes 2001604000
...pageReadIOAndDecompressTime  1199
...pageReadDecodeBytes  2001604000
...pageReadDecodeTime   112
Read 10000000 rows in 1.494 seconds, speed 6.6934404 million rows/s
...footerReadCnt    1
...footerReadTime   2
...pageReadOverallPageCnt   4000
...pageReadOverallCellCnt   40000000
...pageReadOverallTime  1345
...pageReadHeaderCnt    4000
...pageReadHeaderTime   2
...pageReadIOAndDecompressRawIOBytes    2001604000
...pageReadIOAndDecompressBytes 2001604000
...pageReadIOAndDecompressTime  1288
...pageReadDecodeBytes  2001604000
...pageReadDecodeTime   55
Read 10000000 rows in 1.486 seconds, speed 6.729475 million rows/s
...footerReadCnt    1
...footerReadTime   0
...pageReadOverallPageCnt   4000
...pageReadOverallCellCnt   40000000
...pageReadOverallTime  1339
...pageReadHeaderCnt    4000
...pageReadHeaderTime   70
...pageReadIOAndDecompressRawIOBytes    2001604000
...pageReadIOAndDecompressBytes 2001604000
...pageReadIOAndDecompressTime  1131
...pageReadDecodeBytes  2001604000
...pageReadDecodeTime   138
Read 10000000 rows in 1.499 seconds, speed 6.671114 million rows/s

--------------------------------------------------------
Benchmark columns=20, rowSizeBytes=200, rowsPerGroup=1000000
Test file: file:/tmp/StoragePerformanceBenchmark.kap

==== Test write performance ====
Writen 10000000 rows in 20.288 seconds, speed 0.49290222 million rows/s

==== Test Read By Rows ====
Read 10000000 rows in 5.888 seconds, speed 1.6983696 million rows/s
Read 10000000 rows in 5.718 seconds, speed 1.7488632 million rows/s
Read 10000000 rows in 5.903 seconds, speed 1.6940539 million rows/s

==== Test Read By Pages ====
...footerReadCnt    1
...footerReadTime   58
...pageReadOverallPageCnt   20000
...pageReadOverallCellCnt   200000000
...pageReadOverallTime  2005
...pageReadHeaderCnt    20000
...pageReadHeaderTime   144
...pageReadIOAndDecompressRawIOBytes    2008020000
...pageReadIOAndDecompressBytes 2008020000
...pageReadIOAndDecompressTime  1251
...pageReadDecodeBytes  2008020000
...pageReadDecodeTime   608
Read 10000000 rows in 2.796 seconds, speed 3.5765378 million rows/s
...footerReadCnt    1
...footerReadTime   16
...pageReadOverallPageCnt   20000
...pageReadOverallCellCnt   200000000
...pageReadOverallTime  2115
...pageReadHeaderCnt    20000
...pageReadHeaderTime   188
...pageReadIOAndDecompressRawIOBytes    2008020000
...pageReadIOAndDecompressBytes 2008020000
...pageReadIOAndDecompressTime  1217
...pageReadDecodeBytes  2008020000
...pageReadDecodeTime   678
Read 10000000 rows in 2.769 seconds, speed 3.611412 million rows/s
...footerReadCnt    1
...footerReadTime   16
...pageReadOverallPageCnt   20000
...pageReadOverallCellCnt   200000000
...pageReadOverallTime  2069
...pageReadHeaderCnt    20000
...pageReadHeaderTime   248
...pageReadIOAndDecompressRawIOBytes    2008020000
...pageReadIOAndDecompressBytes 2008020000
...pageReadIOAndDecompressTime  1196
...pageReadDecodeBytes  2008020000
...pageReadDecodeTime   625
Read 10000000 rows in 2.807 seconds, speed 3.5625222 million rows/s

============================================================================
Todo thoughts:
- share RawReader (done, no obvious improvement)
- hold metrics in ParquetBundleReader (done, no performance loss, DeltaByteArray is the main penalty as column number grows)
- quit using regex String.split() in ParquetRawReader (done, improve 5% ?)
- quit using DeltaByteArray encoding, use DeltaLengthByteArray (done, size increase 1%, speed improve 100% roughly)

- embed ByteArray iterator in ExpandableByteBuffer
- reduce loops in readByteArray()
- reuse decompressed staging buffer
- read row group as a whole (no need)
 *
 */

public class StoragePerformanceBenchmark {

    final Configuration hadoopConf;

    final int rowSizeBytes;
    final int nCols;
    final int rowsPerGroup = ParquetConfig.PagesPerGroup * ParquetConfig.RowsPerPage;

    final MessageType type;
    final Path path;

    public StoragePerformanceBenchmark(int nCols, int rowSizeBytes) throws IOException {
        this.hadoopConf = HadoopUtil.getCurrentConfiguration();

        this.rowSizeBytes = rowSizeBytes;
        this.nCols = nCols;

        this.type = new MessageType("test", genCols(nCols, rowSizeBytes));
        this.path = HadoopUtil.getWorkingFileSystem().makeQualified(new Path("/tmp/StoragePerformanceBenchmark.kap"));

        System.out.println();
        System.out.println("--------------------------------------------------------");
        System.out.println(
                "Benchmark columns=" + nCols + ", rowSizeBytes=" + rowSizeBytes + ", rowsPerGroup=" + rowsPerGroup);
        System.out.println("Test file: " + path);
    }

    private Type[] genCols(int n, int rowSizeBytes) {
        int colSize = rowSizeBytes / n;
        if (colSize * n != rowSizeBytes)
            throw new IllegalStateException();

        Type[] ret = new Type[n];
        for (int i = 0; i < n; i++) {
            ret[i] = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, colSize,
                    "c" + i);
        }
        return ret;
    }

    public void testWrite(int round) throws Exception {
        System.out.println();
        System.out.println("==== Test write performance ====");
        for (int i = 0; i < round; i++) {
            cleanUpFile();

            Pair<Long, Integer> p = writeRows(10);

            long t = p.getFirst();
            int r = p.getSecond();
            System.out.println("Writen " + r + " rows in " + ((float) t / 1000) + " seconds, speed "
                    + ((float) r / 1000 / t) + " million rows/s");
        }
    }

    private Pair<Long, Integer> writeRows(int groupCnt) throws Exception {
        List<ByteArray> randomBytes = randomBytes(rowsPerGroup, type);

        long t = System.currentTimeMillis();

        int keyLen = type.getColumns().get(0).getTypeLength();
        int[] valueLen = new int[type.getColumns().size() - 1];
        for (int i = 1; i < type.getColumns().size(); i++) {
            valueLen[i - 1] = type.getColumns().get(i).getTypeLength();
        }

        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(hadoopConf).setPath(path).setType(type)
                .build();
        for (int g = 0; g < groupCnt; g++) {
            for (int i = 0; i < rowsPerGroup; i++) {
                byte[] bytes = randomBytes.get(i).array();
                writer.writeRow(bytes, 0, keyLen, bytes, valueLen);
            }
        }

        writer.close();

        return Pair.newPair(System.currentTimeMillis() - t, rowsPerGroup * groupCnt);
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

    public void testReadByRow(int round) throws Exception {
        System.out.println();
        System.out.println("==== Test Read By Rows ====");
        for (int i = 0; i < round; i++) {

            Pair<Long, Integer> p = readRows();

            long t = p.getFirst();
            int r = p.getSecond();
            System.out.println("Read " + r + " rows in " + ((float) t / 1000) + " seconds, speed "
                    + ((float) r / 1000 / t) + " million rows/s");
        }
    }

    private Pair<Long, Integer> readRows() throws IOException {
        long t = System.currentTimeMillis();

        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(hadoopConf).build();

        int cnt = 0;
        ArrayList<Object> row = new ArrayList<>();
        while (bundleReader.read(row) != null) {
            cnt++;
            continue;
        }

        bundleReader.close();

        return Pair.newPair(System.currentTimeMillis() - t, cnt);
    }

    public void testReadByPage(int round) throws Exception {
        System.out.println();
        System.out.println("==== Test Read By Pages ====");
        for (int i = 0; i < round; i++) {

            Pair<Long, Integer> p = readPages();

            long t = p.getFirst();
            int r = p.getSecond();
            System.out.println("Read " + r + " rows in " + ((float) t / 1000) + " seconds, speed "
                    + ((float) r / 1000 / t) + " million rows/s");
        }
    }

    private Pair<Long, Integer> readPages() throws IOException {
        long t = System.currentTimeMillis();
        
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(hadoopConf).build();

        int cnt = 0;
        while (bundleReader.readByteArray() != null) {
            cnt++;
            continue;
        }

        bundleReader.close();
        
        System.out.println(bundleReader.getMetrics().summary());
        bundleReader.getMetrics().reset();
        return Pair.newPair(System.currentTimeMillis() - t, cnt);
    }

    private void cleanUpFile() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static void main(String[] args) throws Exception {
        test(1, 200);
        test(4, 200);
        test(20, 200);
    }

    private static void test(int nCols, int rowSizeBytes) throws Exception {
        StoragePerformanceBenchmark benchmark = new StoragePerformanceBenchmark(nCols, rowSizeBytes);
        try {
            benchmark.testWrite(1);
            benchmark.testReadByRow(3);
            benchmark.testReadByPage(3);
        } finally {
            benchmark.cleanUpFile();
        }
    }
}
