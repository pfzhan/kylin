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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ParquetPageIndexTableTest {
    final static int dataSize = 50;
    final static int maxVal = dataSize * 2;
    final static int cardinality = dataSize;
    static ParquetPageIndexTable indexTable;
    static ParquetPageIndexTable indexOrderedTable;
    static int[] data1;
    static int[] data2;
    static int[] data3;
    static int columnLength = 1 + (Integer.SIZE - Integer.numberOfLeadingZeros(maxVal)) / Byte.SIZE;

    static String[] columnName = {"odd", "even", "only"};
    static boolean[] onlyEq = {false, false, true};
    static TblColRef colRef1;
    static TblColRef colRef2;
    static TblColRef colRef3;


    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();

        File indexFile = File.createTempFile("local", "inv");
        writeIndexFile(indexFile);
        Path invPath = new Path(indexFile.getAbsolutePath());
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        FSDataInputStream inputStream = fileSystem.open(invPath);
        indexTable = new ParquetPageIndexTable(fileSystem, invPath, inputStream, 0);
        inputStream.seek(0);
        indexOrderedTable = new ParquetOrderedPageIndexTable(fileSystem, invPath, inputStream, 0, Collections.singleton(0));

        colRef1 = TblColRef.mockup(null, 1, columnName[0], null);
        colRef2 = TblColRef.mockup(null, 2, columnName[1], null);
        colRef3 = TblColRef.mockup(null, 3, columnName[2], null);
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();

        indexTable.close();
        indexOrderedTable.close();
    }


    private static void writeIndexFile(File indexFile) throws IOException {
        data1 = new int[dataSize];
        data2 = new int[dataSize];
        data3 = new int[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data1[i] = 2 * i;
            data2[i] = 2 * i + 1;
            data3[i] = 2 * i + 1;
        }
        int[] cardinalities = {cardinality, cardinality, cardinality};
        int[] columnLengthes = {columnLength, columnLength, columnLength};
        ColumnSpec[] specs = new ColumnSpec[3];
        for (int i = 0; i < 3; i++) {
            specs[0] = new ColumnSpec(columnName[i], columnLength, cardinality, onlyEq[i], i);
            specs[0].setKeyEncodingIdentifier('a');
            specs[0].setValueEncodingIdentifier('s');
        }
        ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLengthes, cardinalities, onlyEq, new DataOutputStream(new FileOutputStream(indexFile)));
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer = new byte[columnLength * 3];
            BytesUtil.writeUnsigned(data1[i], buffer, 0, columnLength);
            BytesUtil.writeUnsigned(data2[i], buffer, columnLength, columnLength);
            BytesUtil.writeUnsigned(data3[i], buffer, columnLength * 2, columnLength);

            writer.write(buffer, i);
        }
        writer.close();
    }

    private static TupleFilter makeFilter(TupleFilter.FilterOperatorEnum opt, TblColRef col, int value) {
        TupleFilter filter = new CompareTupleFilter(opt);
        filter.addChild(new ColumnTupleFilter(col));
        byte[] buffer = new byte[columnLength];
        BytesUtil.writeUnsigned(value, buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        return filter;
    }

    private static int[] rangeInts(int from, int to) {
        return rangeInts(from, to, null);
    }

    private static int[] rangeInts(int from, int to, int... excludes) {
        Set<Integer> excludeSet = Sets.newHashSet();
        if (excludes != null) {
            for (int i = 0; i < excludes.length; i++) {
                excludeSet.add(excludes[i]);
            }
        }
        ArrayList<Integer> allInts = Lists.newArrayList();
        for (int i = from; i <= to; i++) {
            allInts.add(i);
        }
        allInts.removeAll(excludeSet);
        Integer[] result = new Integer[allInts.size()];
        return ArrayUtils.toPrimitive(allInts.toArray(result));
    }

    @Test
    public void testOnlyEQ() {
        TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef3, 10);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.NEQ, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.NOTIN, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef3, 10);
        result = indexTable.lookup(filter);
        assertEquals(0, result.toArray().length);

        filter = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef3, 3);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(1, 1), result.toArray());

        filter = makeFilter(TupleFilter.FilterOperatorEnum.ISNULL, colRef3, 10);
        result = indexTable.lookup(filter);
        assertEquals(0, result.toArray().length);
    }

    @Test
    public void testEQ() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(i, i), result.toArray());
        }
    }

    @Test
    public void testNullFilter() {
        ImmutableRoaringBitmap result = indexTable.lookup(null);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testConstantTrueFilter() {
        ImmutableRoaringBitmap result = indexTable.lookup(ConstantTupleFilter.TRUE);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testConstantFalseFilter() {
        ImmutableRoaringBitmap result = indexTable.lookup(ConstantTupleFilter.FALSE);
        assertArrayEquals(new int[0], result.toArray());
    }

    @Test
    public void testISNULL() throws IOException {
        TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.ISNULL, colRef1, data1[0]);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertEquals(0, result.getCardinality());
    }

    @Test
    public void testISNOTNULL() throws IOException {
        TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL, colRef1, data1[0]);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testGTE1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testEqEqualGTE1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testGT1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testEqRoundGT1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testGTE2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testEqRoundGTE2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testGT2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testEqRoundGT2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testLT1() throws IOException {
        for (int i = 1; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testEqRoundLT1() throws IOException {
        for (int i = 1; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testLTE1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testEqRoundLTE1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testLT2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testEqRoundLT2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testLTE2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testEqRoundLTE2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexOrderedTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i), result.toArray());
        }
    }

    @Test
    public void testLTEMax() throws IOException {
        TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef2, Integer.MAX_VALUE);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testGTEMin() throws IOException {
        TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef2, 0);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testIn1() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.IN);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer1 = new byte[columnLength];
        BytesUtil.writeUnsigned(0, buffer1, 0, buffer1.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer1)));
        byte[] buffer2 = new byte[columnLength];
        BytesUtil.writeUnsigned(10, buffer2, 0, buffer2.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer2)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[2];
        expected[0] = 0;
        expected[1] = 5;
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testIn2() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.IN);
        filter.addChild(new ColumnTupleFilter(colRef2));
        byte[] buffer1 = new byte[columnLength];
        BytesUtil.writeUnsigned(1, buffer1, 0, buffer1.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer1)));
        byte[] buffer2 = new byte[columnLength];
        BytesUtil.writeUnsigned(2, buffer2, 0, buffer2.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer2)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[1];
        expected[0] = 0;
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testOr1() {
        TupleFilter filter1 = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef1, data1[0]);
        TupleFilter filter2 = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef1, data1[5]);
        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        filter.addChild(filter1);
        filter.addChild(filter2);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(new int[]{0, 5}, result.toArray());
    }

    @Test
    public void testOr2() {
        TupleFilter filter1 = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef1, data1[5]);
        TupleFilter filter2 = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef1, data1[5]);
        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        filter.addChild(filter1);
        filter.addChild(filter2);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testAnd1() {
        TupleFilter filter1 = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef2, data2[0]);
        TupleFilter filter2 = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef1, data1[5]);
        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        filter.addChild(filter1);
        filter.addChild(filter2);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertEquals(0, result.getCardinality());
    }

    @Test
    public void testAnd2() {
        TupleFilter filter1 = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef1, data1[5]);
        TupleFilter filter2 = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef2, data2[25]);
        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        filter.addChild(filter1);
        filter.addChild(filter2);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(5, 25), result.toArray());
    }

    @Test
    public void testAndOR1() {
        TupleFilter filter1_1 = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef1, data1[5]);
        TupleFilter filter1_2 = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef2, data2[25]);
        TupleFilter filter1 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        filter1.addChild(filter1_1);
        filter1.addChild(filter1_2);

        TupleFilter filter2_1 = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef2, data2[30]);
        TupleFilter filter2_2 = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef1, data1[20]);
        TupleFilter filter2 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        filter2.addChild(filter2_1);
        filter2.addChild(filter2_2);

        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        filter.addChild(filter1);
        filter.addChild(filter2);

        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(5, 30), result.toArray());
    }

    @Test
    public void testAndOR2() {
        TupleFilter filter1_1 = makeFilter(TupleFilter.FilterOperatorEnum.GTE, colRef1, data1[25]);
        TupleFilter filter1_2 = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef2, data2[5]);
        TupleFilter filter1 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        filter1.addChild(filter1_1);
        filter1.addChild(filter1_2);

        TupleFilter filter2_1 = makeFilter(TupleFilter.FilterOperatorEnum.LTE, colRef2, data2[22]);
        TupleFilter filter2_2 = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef1, data1[24]);
        TupleFilter filter2 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        filter2.addChild(filter2_1);
        filter2.addChild(filter2_2);

        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        filter.addChild(filter1);
        filter.addChild(filter2);

        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1, rangeInts(6, 24)), result.toArray());
    }
}
