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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.common.collect.Iterables;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

/**
 * Created by dong on 6/21/16.
 */
public class ParquetPageIndexTableTest extends LocalFileMetadataTestCase {
    File indexFile;
    FSDataInputStream inputStream;

    final int dataSize = 50;
    final int maxVal = 100;
    final int cardinality = 50;

    int[] data1;
    int[] data2;
    int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(maxVal), Integer.SIZE - Integer.numberOfLeadingZeros(maxVal) };
    int[] cardinalities = { cardinality, cardinality };
    String[] columnName = { "odd", "even" };
    boolean[] onlyEq = { false, false };

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Before
    public void setUp() throws Exception {
        Log4jConfigurer.initLogger();
        createTestMetadata();

        indexFile = File.createTempFile("local", "inv");
        writeIndexFile();
        inputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).open(new Path(indexFile.getAbsolutePath()));
    }

    private void writeIndexFile() throws IOException {
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
        ParquetPageIndexTable indexTable = new ParquetPageIndexTable(inputStream);

        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        Iterable<Integer> result = indexTable.lookup(filter);
        assertEquals(0, Iterables.getOnlyElement(result).intValue());
    }

    @Test
    public void testLTE() throws IOException {
        ParquetPageIndexTable indexTable = new ParquetPageIndexTable(inputStream);

        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        Iterable<Integer> result = indexTable.lookup(filter);
        assertEquals(0, Iterables.getOnlyElement(result).intValue());
    }

    @Test
    public void testGTE() throws IOException {
        ParquetPageIndexTable indexTable = new ParquetPageIndexTable(inputStream);

        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[50];
        for (int i = 0; i < 50; i++) {
            expected[i] = i;
        }
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testGT() throws IOException {
        ParquetPageIndexTable indexTable = new ParquetPageIndexTable(inputStream);

        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[0], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[49];
        for (int i = 0; i < 49; i++) {
            expected[i] = i + 1;
        }
        assertArrayEquals(expected, result.toArray());
    }

    @Test
    public void testLT() throws IOException {
        ParquetPageIndexTable indexTable = new ParquetPageIndexTable(inputStream);

        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        byte[] buffer = new byte[columnLength[0]];
        BytesUtil.writeUnsigned(data1[1], buffer, 0, buffer.length);
        filter.addChild(new ConstantTupleFilter(new ByteArray(buffer)));
        ImmutableRoaringBitmap result = indexTable.lookup(filter);
        int[] expected = new int[1];
        expected[0] = 0;
        assertArrayEquals(expected, result.toArray());
    }
}
