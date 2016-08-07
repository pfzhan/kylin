package io.kyligence.kap.storage.parquet.format.pageIndex;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.IndexMapCache;

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
        System.setProperty("kap.parquet.ii.spill.threshold", "79");
        testIndexMapCache();
    }

    @Test
    public void testNoSpill() {
        System.setProperty("kap.parquet.ii.spill.threshold", Integer.toString(Integer.MAX_VALUE));
        testIndexMapCache();
    }

    private void testIndexMapCache() {
        int dataSize = 1342;
        int columnLength = 8;
        int pageNum = 100;

        IndexMapCache indexMapCache = new IndexMapCache(true);
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
        for (Pair<ByteArray, MutableRoaringBitmap> val : indexMapCache.getIterable(true)) {
            assertEquals(num++, BytesUtil.readUnsigned(val.getFirst().array(), val.getFirst().offset(), val.getFirst().length()));
        }

        assertEquals(num, dataSize);
    }
}
