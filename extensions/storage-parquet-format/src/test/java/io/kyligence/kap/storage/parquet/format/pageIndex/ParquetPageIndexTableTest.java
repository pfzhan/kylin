package io.kyligence.kap.storage.parquet.format.pageIndex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.common.collect.Iterables;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class ParquetPageIndexTableTest extends LocalFileMetadataTestCase {
    static ParquetPageIndexTable indexTable;
    final static int dataSize = 50;
    final static int last = dataSize - 1;
    final static int maxVal = 100;
    final static int cardinality = 50;

    static int[] data1;
    static int[] data2;
    static int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(maxVal), Integer.SIZE - Integer.numberOfLeadingZeros(maxVal) };
    static int[] cardinalities = { cardinality, cardinality };
    static String[] columnName = { "odd", "even" };
    static boolean[] onlyEq = { false, false };
    static TblColRef colRef1;
    static TblColRef colRef2;

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
        indexTable.close();
        //        inputStream.close();
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
    }

    private static void writeIndexFile(File indexFile) throws IOException {
        data1 = new int[dataSize];
        data2 = new int[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data1[i] = 2 * i;
            data2[i] = 2 * i + 1;
        }

        ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLength, cardinalities, onlyEq, new DataOutputStream(new FileOutputStream(indexFile)));
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer = new byte[columnLength[0] + columnLength[1]];
            BytesUtil.writeUnsigned(data1[i], buffer, 0, columnLength[0]);
            BytesUtil.writeUnsigned(data2[i], buffer, columnLength[0], columnLength[1]);
            writer.write(buffer, 0, i);
        }
        writer.close();
    }

    @Test
    public void testEQ() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        Iterable<Integer> result = indexTable.lookup(filter);
        assertEquals(0, Iterables.getOnlyElement(result).intValue());
    }

    @Test
    public void testLTE() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        Iterable<Integer> result = indexTable.lookup(filter);
        assertEquals(0, Iterables.getOnlyElement(result).intValue());
    }

    @Test
    public void testGTE() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[dataSize];
        for (int i = 0; i < dataSize; i++) {
            expected[i] = i;
        }
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testGT() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[last];
        for (int i = 0; i < last; i++) {
            expected[i] = i + 1;
        }
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testGTEMax() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[last], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[1];
        expected[0] = last;
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testGTMax() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[last], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[0];
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testGTMax2() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        filter.addChild(new ColumnTupleFilter(colRef2));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data2[last], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[0];
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testLT() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[10], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[10];
        for (int i = 0; i < 10; i++) {
            expected[i] = i;
        }
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testLTMin1_1() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[0];
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testLTMin1_2() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
        filter.addChild(new ColumnTupleFilter(colRef1));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(1, buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[1];
        expected[0] = 0;
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testLTMin2_1() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
        filter.addChild(new ColumnTupleFilter(colRef2));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data2[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[0];
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testLTMin2_2() throws IOException {
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
        filter.addChild(new ColumnTupleFilter(colRef2));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(0, buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[0];
        assertArrayEquals(expected, result.toArray());
    }
}
