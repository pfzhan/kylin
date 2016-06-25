package io.kyligence.kap.cube.gridtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.BitSet;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTSampleCodeSystem;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class GTScanRangesTest extends LocalFileMetadataTestCase {
    static GTInfo INFO;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
        INFO = info();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    private static final GTInfo info() {
        GTInfo.Builder builder = GTInfo.builder();
        builder.setCodeSystem(new GTSampleCodeSystem());
        builder.setColumns(DataType.getType("bigint"));
        BitSet set = new BitSet();
        set.set(0);
        builder.setPrimaryKey(new ImmutableBitSet(set));
        builder.setColumnPreferIndex(new ImmutableBitSet(set));
        return builder.build();
    }

    @Test
    public void testBasic() {
        GTRecord[] starts = { newRec(new LongMutable(10)), newRec(new LongMutable(5)) };
        GTRecord[] ends = { newRec(new LongMutable(30)), newRec(new LongMutable(50)) };
        int[] rowCounts = { 10, 20 };
        GTScanRanges scanRanges = new GTScanRanges(starts, ends, rowCounts);
        assertEquals("[[5],[50]]", scanRanges.toString());
    }

    @Test
    public void testAddRange() {
        GTRecord[] starts = { newRec(new LongMutable(10)), newRec(new LongMutable(50)) };
        GTRecord[] ends = { newRec(new LongMutable(30)), newRec(new LongMutable(80)) };
        int[] rowCounts = { 10, 20 };
        GTScanRanges scanRanges = new GTScanRanges(starts, ends, rowCounts);

        // add to head without connection
        scanRanges.addScanRage(new GTScanRanges.ScanRange(newRec(new LongMutable(8)), newRec(new LongMutable(9)), 0));
        assertEquals("[[8],[9]],[[10],[30]],[[50],[80]]", scanRanges.toString());

        // add to middle with connection
        scanRanges.addScanRage(new GTScanRanges.ScanRange(newRec(new LongMutable(20)), newRec(new LongMutable(40)), 0));
        assertEquals("[[8],[9]],[[10],[40]],[[50],[80]]", scanRanges.toString());

        // connect with two ranges
        scanRanges.addScanRage(new GTScanRanges.ScanRange(newRec(new LongMutable(10)), newRec(new LongMutable(50)), 0));
        assertEquals("[[8],[9]],[[10],[80]]", scanRanges.toString());

        // connect with all ranges
        scanRanges.addScanRage(new GTScanRanges.ScanRange(newRec(new LongMutable(0)), newRec(new LongMutable(100)), 0));
        assertEquals("[[0],[100]]", scanRanges.toString());
    }

    @Test
    public void testNot() {
        GTRecord[] starts1 = { newRec(new LongMutable(10)), newRec(new LongMutable(50)) };
        GTRecord[] ends1 = { newRec(new LongMutable(30)), newRec(new LongMutable(80)) };
        int[] rowCounts1 = { 10, 20 };
        GTScanRanges scanRanges1 = new GTScanRanges(starts1, ends1, rowCounts1);
        assertEquals("[[null],[10]],[[30],[50]],[[80],[null]]", scanRanges1.not().toString());
    }

    @Test
    public void testAnd() {
        GTScanRanges scanRanges0 = new GTScanRanges(null, null, null);
        assertTrue(StringUtils.isEmpty(scanRanges0.and(scanRanges0).toString()));

        GTRecord[] starts1 = { newRec(new LongMutable(10)), newRec(new LongMutable(50)) };
        GTRecord[] ends1 = { newRec(new LongMutable(30)), newRec(new LongMutable(80)) };
        int[] rowCounts1 = { 10, 20 };
        GTScanRanges scanRanges1 = new GTScanRanges(starts1, ends1, rowCounts1);

        assertTrue(StringUtils.isEmpty(scanRanges0.and(scanRanges1).toString()));

        GTRecord[] starts2 = { newRec(new LongMutable(1)), newRec(new LongMutable(5)), newRec(new LongMutable(20)), newRec(new LongMutable(75)) };
        GTRecord[] ends2 = { newRec(new LongMutable(2)), newRec(new LongMutable(15)), newRec(new LongMutable(70)), newRec(new LongMutable(110)) };
        int[] rowCounts2 = { 1, 10, 20, 30 };
        GTScanRanges scanRanges2 = new GTScanRanges(starts2, ends2, rowCounts2);

        assertEquals("[[10],[30]],[[50],[80]]", scanRanges1.and(scanRanges1).toString());
        assertEquals("[[10],[15]],[[20],[30]],[[50],[70]],[[75],[80]]", scanRanges1.and(scanRanges1).and(scanRanges2).toString());
        assertEquals(scanRanges2.and(scanRanges1).toString(), scanRanges1.and(scanRanges2).toString());
    }

    @Test
    public void testOr() {
        GTRecord[] starts1 = { newRec(new LongMutable(10)), newRec(new LongMutable(50)) };
        GTRecord[] ends1 = { newRec(new LongMutable(30)), newRec(new LongMutable(80)) };
        int[] rowCounts1 = { 10, 20 };
        GTScanRanges scanRanges1 = new GTScanRanges(starts1, ends1, rowCounts1);

        GTRecord[] starts2 = { newRec(new LongMutable(1)), newRec(new LongMutable(5)), newRec(new LongMutable(20)), newRec(new LongMutable(75)) };
        GTRecord[] ends2 = { newRec(new LongMutable(2)), newRec(new LongMutable(15)), newRec(new LongMutable(70)), newRec(new LongMutable(110)) };
        int[] rowCounts2 = { 1, 10, 20, 30 };
        GTScanRanges scanRanges2 = new GTScanRanges(starts2, ends2, rowCounts2);

        assertEquals("[[10],[30]],[[50],[80]]", scanRanges1.or(scanRanges1).toString());
        assertEquals("[[1],[2]],[[5],[110]]", scanRanges1.or(scanRanges2).toString());
        assertEquals(scanRanges1.or(scanRanges2).toString(), scanRanges2.or(scanRanges1).toString());
    }

    @Test
    @Ignore // ignore this test because cannot tell MAX and MIN for GTRecord currently
    public void testLogicalCal() {
        GTRecord[] starts1 = { newRec(new LongMutable(10)), newRec(new LongMutable(50)) };
        GTRecord[] ends1 = { newRec(new LongMutable(30)), newRec(new LongMutable(80)) };
        int[] rowCounts1 = { 10, 20 };
        GTScanRanges scanRanges1 = new GTScanRanges(starts1, ends1, rowCounts1);

        GTRecord[] starts2 = { newRec(new LongMutable(1)), newRec(new LongMutable(5)), newRec(new LongMutable(20)), newRec(new LongMutable(75)) };
        GTRecord[] ends2 = { newRec(new LongMutable(2)), newRec(new LongMutable(15)), newRec(new LongMutable(70)), newRec(new LongMutable(110)) };
        int[] rowCounts2 = { 1, 10, 20, 30 };
        GTScanRanges scanRanges2 = new GTScanRanges(starts2, ends2, rowCounts2);

        System.out.println(scanRanges1.and(scanRanges2).not().toString());
        System.out.println(scanRanges1.not().or(scanRanges2.not()).toString());
    }

    private GTRecord newRec(Object... values) {
        GTRecord rec = new GTRecord(INFO);
        return rec.setValues(values);
    }
}
