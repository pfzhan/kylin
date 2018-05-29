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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.IndexMapCache;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IntEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.MutableRoaringBitmapEncoding;

public class IndexMapCacheTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testSpill() {
        System.setProperty("kap.storage.columnar.ii-spill-threshold-mb", Integer.toString(Integer.MAX_VALUE));
        testIndexMapCache();
        System.clearProperty("kap.storage.columnar.ii-spill-threshold-mb");
    }

    @Test
    public void testNoSpill() {
        System.setProperty("kap.storage.columnar.ii-spill-threshold-mb", Integer.toString(Integer.MIN_VALUE));
        testIndexMapCache();
        System.clearProperty("kap.storage.columnar.ii-spill-threshold-mb");
    }

    private void createGarbage() {
        byte[] garbage = new byte[512 * 1024 * 1024];
        garbage[0] = garbage[garbage.length - 1] = 127;
    }

    private void testIndexMapCache() {
        int dataSize = 2;
        int columnLength = 8;
        int pageNum = 2;

        IndexMapCache indexMapCache = new IndexMapCache("test", true, new IntEncoding(),
                new MutableRoaringBitmapEncoding(), true);
        for (int c = 0; c < 3; c++) {
            for (int i = 0; i < dataSize; i++) {
                for (int j = 0; j < pageNum; j++) {
                    // create garbage to bring progress after gc.
                    createGarbage();

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
            List<? extends Number> vals = Lists.newArrayList(val.getSecond());
            assertEquals(0, vals.get(0));
            assertEquals(1, vals.get(1));
        }

        assertEquals(num, dataSize);

        indexMapCache.close();
    }
}
