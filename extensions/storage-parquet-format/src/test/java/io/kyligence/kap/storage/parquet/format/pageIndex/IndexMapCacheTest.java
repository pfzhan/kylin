package io.kyligence.kap.storage.parquet.format.pageIndex;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.IndexMapCache;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IntEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.MutableRoaringBitmapEncoding;

public class IndexMapCacheTest extends LocalFileMetadataTestCase {
    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testSpill() {
        System.setProperty("kap.storage.columnar.ii.spill.threshold.mb", Integer.toString(Integer.MAX_VALUE));
        testIndexMapCache();
        System.clearProperty("kap.storage.columnar.ii.spill.threshold.mb");
    }

    @Test
    public void testNoSpill() {
        System.setProperty("kap.storage.columnar.ii.spill.threshold.mb", Integer.toString(Integer.MIN_VALUE));
        testIndexMapCache();
        System.clearProperty("kap.storage.columnar.ii.spill.threshold.mb");
    }

    private void testIndexMapCache() {
        int dataSize = 10;
        int columnLength = 8;
        int pageNum = 10;

        IndexMapCache indexMapCache = new IndexMapCache("test", true, new IntEncoding(), new MutableRoaringBitmapEncoding(), true);
        for (int c = 0; c < 3; c++) {
            for (int i = 0; i < dataSize; i++) {
                for (int j = 0; j < pageNum; j++) {
                    ByteArray key = ByteArray.allocate(columnLength);
                    BytesUtil.writeUnsigned(i, key.array(), key.offset(), key.length());
                    indexMapCache.put(key, j);
                }
            }
        }
        assertEquals(dataSize, indexMapCache.size());

        int num = 0;
        for (Pair<Comparable, ? extends Iterable<? extends Number>> val : indexMapCache.getIterable(true)) {
            assertEquals(num++, val.getFirst());
        }

        assertEquals(num, dataSize);
    }
}
