package io.kyligence.kap.storage.parquet.format.raw;

import org.apache.kylin.common.util.ByteArray;
import org.junit.Assert;
import org.junit.Test;

public class RawTableUtilsTest {
    @Test
    public void shrinkTest() {
        byte[] origin = new byte[] { 0x10 };
        byte[] expected = new byte[] { 0x40 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 8));

        origin = new byte[] { 0x10, 0x13 };
        ByteArray byteArray = new ByteArray(new byte[] { 0x4c, 0x10, 0x13 }, 1, 2);
        expected = new byte[] { 0x41, 0x30 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 16));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 16).toBytes());

        expected = new byte[] { 0x41, 0x20 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 11));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 11).toBytes());

        expected = new byte[] { 0x71 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 8));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 8).toBytes());

        expected = new byte[] { 0x70 };
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(origin, 7));
        Assert.assertArrayEquals(expected, RawTableUtils.shrink(byteArray, 7).toBytes());
    }
}
