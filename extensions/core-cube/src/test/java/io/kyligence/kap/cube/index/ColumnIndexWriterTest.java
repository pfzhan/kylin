package io.kyligence.kap.cube.index;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.kylin.common.util.BytesUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ColumnIndexWriterTest {
    private static final Logger logger = LoggerFactory.getLogger(ColumnIndexWriterTest.class);

    private static int COL1_MAX_VALUE = 1000;
    private static int COL1_LENGTH = Integer.SIZE - Integer.numberOfLeadingZeros(COL1_MAX_VALUE);
    private static int COL2_MAX_VALUE = 500;
    private static int COL2_LENGTH = Integer.SIZE - Integer.numberOfLeadingZeros(COL2_MAX_VALUE);

    @Test
    public void test() throws IOException {
        File COL1_FWD_IDX = File.createTempFile("COL1", ".fwd");
        File COL1_INV_IDX = File.createTempFile("COL1", ".inv");

        File COL2_FWD_IDX = File.createTempFile("COL2", ".fwd");
        File COL2_INV_IDX = File.createTempFile("COL2", ".inv");

        System.out.println("COL1_FWD_IDX: " + COL1_FWD_IDX.getAbsolutePath());
        System.out.println("COL1_INV_IDX: " + COL1_INV_IDX.getAbsolutePath());

        COL1_FWD_IDX.deleteOnExit();
        COL1_INV_IDX.deleteOnExit();
        COL2_FWD_IDX.deleteOnExit();
        COL2_INV_IDX.deleteOnExit();

        int LENGTH_OF_TABLE = 10000;
        int[] col1Values = new int[LENGTH_OF_TABLE];
        int[] col2Values = new int[LENGTH_OF_TABLE];

        // generate the data;
        Random r = new Random();
        for (int i = 0; i < LENGTH_OF_TABLE; i++) {
            col1Values[i] = r.nextInt(COL1_MAX_VALUE);
            col2Values[i] = r.nextInt(COL2_MAX_VALUE);
        }

        // concat col1 + col2, write to the index
        try (ColumnIndexWriter writer1 = new ColumnIndexWriter("COL1", COL1_MAX_VALUE, COL1_MAX_VALUE, 0, COL1_LENGTH, COL1_FWD_IDX, COL1_INV_IDX); ColumnIndexWriter writer2 = new ColumnIndexWriter("COL2", COL2_MAX_VALUE, COL2_MAX_VALUE, COL1_LENGTH, COL2_LENGTH, COL2_FWD_IDX, COL2_INV_IDX);) {
            for (int i = 0; i < LENGTH_OF_TABLE; i++) {
                byte[] row = new byte[COL1_LENGTH + COL2_LENGTH];
                BytesUtil.writeUnsigned(col1Values[i], row, 0, COL1_LENGTH);
                BytesUtil.writeUnsigned(col2Values[i], row, COL1_LENGTH, COL2_LENGTH);
                writer1.write(row);
                writer2.write(row);
            }
        }

        // read and verify forward index
        try (IColumnForwardIndex.Reader reader1 = ColumnIndexFactory.createLocalForwardIndex("COL1", COL1_MAX_VALUE, COL1_FWD_IDX.getAbsolutePath()).getReader(); IColumnForwardIndex.Reader reader2 = ColumnIndexFactory.createLocalForwardIndex("COL2", COL2_MAX_VALUE, COL2_FWD_IDX.getAbsolutePath()).getReader();) {
            org.junit.Assert.assertEquals(reader1.getNumberOfRows(), LENGTH_OF_TABLE);
            for (int i = 0; i < LENGTH_OF_TABLE; i++) {
                org.junit.Assert.assertTrue(reader1.get(i) == col1Values[i]);
                org.junit.Assert.assertTrue(reader2.get(i) == col2Values[i]);
            }
        }

        // read and verify inverted index
        try (IColumnInvertedIndex.Reader reader1 = ColumnIndexFactory.createLocalInvertedIndex("COL1", COL1_MAX_VALUE, COL1_INV_IDX.getAbsolutePath()).getReader(); IColumnInvertedIndex.Reader reader2 = ColumnIndexFactory.createLocalInvertedIndex("COL2", COL2_MAX_VALUE, COL2_INV_IDX.getAbsolutePath()).getReader();) {
            org.junit.Assert.assertEquals(reader1.getNumberOfRows(), COL1_MAX_VALUE);
            org.junit.Assert.assertEquals(reader2.getNumberOfRows(), COL2_MAX_VALUE);
            for (int i = 0; i < LENGTH_OF_TABLE; i++) {
                org.junit.Assert.assertTrue(reader1.getRows(col1Values[i]).contains(i));
                org.junit.Assert.assertTrue(reader2.getRows(col2Values[i]).contains(i));
            }
        }

    }
}