package io.kyligence.kap.storage.parquet.format.pageIndex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Log4jConfigurer;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class ParquetPageIndexTableTest extends LocalFileMetadataTestCase {
    final static int dataSize = 500;
    final static int maxVal = dataSize * 2;
    final static int cardinality = dataSize;
    static ParquetPageIndexTable indexTable;
    static int[] data1;
    static int[] data2;
    static int[] data3;
    static int columnLength = Integer.SIZE - Integer.numberOfLeadingZeros(maxVal);

    static String[] columnName = { "odd", "even", "only" };
    static boolean[] onlyEq = { false, false, true };
    static TblColRef colRef1;
    static TblColRef colRef2;
    static TblColRef colRef3;

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
        indexTable.close();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Log4jConfigurer.initLogger();

        staticCreateTestMetadata();

        File indexFile = File.createTempFile("local", "inv");
        writeIndexFile(indexFile);
        FSDataInputStream inputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).open(new Path(indexFile.getAbsolutePath()));
        indexTable = new ParquetPageIndexTable(inputStream);

        colRef1 = ColumnDesc.mockup(null, 1, columnName[0], null).getRef();
        colRef2 = ColumnDesc.mockup(null, 2, columnName[1], null).getRef();
        colRef3 = ColumnDesc.mockup(null, 3, columnName[2], null).getRef();
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
        int[] cardinalities = { cardinality, cardinality, cardinality };
        int[] columnLengthes = { columnLength, columnLength, columnLength };
        ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLengthes, cardinalities, onlyEq, new DataOutputStream(new FileOutputStream(indexFile)));
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer = new byte[columnLength * 3];
            BytesUtil.writeUnsigned(data1[i], buffer, 0, columnLength);
            BytesUtil.writeUnsigned(data2[i], buffer, columnLength, columnLength);
            BytesUtil.writeUnsigned(data3[i], buffer, columnLength * 2, columnLength);
            writer.write(buffer, 0, i);
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

        filter = makeFilter(TupleFilter.FilterOperatorEnum.NEQ, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());


        filter = makeFilter(TupleFilter.FilterOperatorEnum.NOTIN, colRef3, 10);
        result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
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
    public void testNEQ1() throws IOException {
        for (int i = 1; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.NEQ, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, dataSize - 1, i), result.toArray());
        }
    }

    @Test
    public void testNEQ1_NEQ0() throws IOException {
        // currently lt 0 will become lte 0, because ByteArray of -1 is larger.
        TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.NEQ, colRef1, data1[0]);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1), result.toArray());
    }

    @Test
    public void testNEQ2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.NEQ, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, dataSize - 1, i), result.toArray());
        }
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
    public void testGT1() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(1 + i, dataSize - 1), result.toArray());
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
    public void testGT2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.GT, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(1 + i, dataSize - 1), result.toArray());
        }
    }

    @Test
    public void testLT1() throws IOException {
        for (int i = 1; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef1, data1[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i - 1), result.toArray());
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
    public void testLT2() throws IOException {
        for (int i = 0; i < dataSize; i++) {
            TupleFilter filter = makeFilter(TupleFilter.FilterOperatorEnum.LT, colRef2, data2[i]);
            ImmutableRoaringBitmap result = indexTable.lookup(filter);
            assertArrayEquals(rangeInts(0, i - 1), result.toArray());
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
    public void testNotIn() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.NOTIN);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer1 = new byte[columnLength];
        BytesUtil.writeUnsigned(50, buffer1, 0, buffer1.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer1)));
        byte[] buffer2 = new byte[columnLength];
        BytesUtil.writeUnsigned(28, buffer2, 0, buffer2.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer2)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(rangeInts(0, dataSize - 1, 50 / 2, 28 / 2), result.toArray());
    }

    @Test
    public void testOr1() {
        TupleFilter filter1 = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef1, data1[0]);
        TupleFilter filter2 = makeFilter(TupleFilter.FilterOperatorEnum.EQ, colRef1, data1[5]);
        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        filter.addChild(filter1);
        filter.addChild(filter2);
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        assertArrayEquals(new int[] { 0, 5 }, result.toArray());
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
        assertArrayEquals(rangeInts(5, 24), result.toArray());
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
        assertArrayEquals(rangeInts(0, dataSize - 1, rangeInts(5, 24)), result.toArray());
    }
}
