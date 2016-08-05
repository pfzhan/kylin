package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.util.HashSet;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.EncodingType;
import org.junit.Ignore;
import org.junit.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Sets;

@Ignore
public class BitmapTest {

    @Test
    public void testBitmap() throws InterruptedException {
        MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf();
        HashSet<Short> intSet = Sets.newHashSet();
        for (int i = 0; i < 4103; i++) {
            bitmap.add(i * 5);
            intSet.add((short) (i * 5));
        }
        bitmap.runOptimize();
        System.out.println(bitmap.serializedSizeInBytes());
        System.out.print(2 * (intSet.size() + 1));

//        Thread.sleep(60 * 1000L);

    }
}
