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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexReader;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;

public class ColumnIndexTest {
    Path indexPath = new Path("/tmp/testkylin/a.inv");

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
    public void testRoundTrip() throws IOException {
        // prepare data
        Map<ByteArray, Integer> data = Maps.newLinkedHashMap();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 1; k++) {
                    data.put(new ByteArray(new byte[] { (byte) i }), i);
                }
            }
        }

        // write
        FSDataOutputStream outputStream = HadoopUtil.getWorkingFileSystem().create(indexPath);
        ColumnIndexWriter indexWriter = new ColumnIndexWriter(new ColumnSpec("0", 1, 0, false, 0), outputStream);
        for (Map.Entry<ByteArray, Integer> dataEntry : data.entrySet()) {
            indexWriter.appendToRow(dataEntry.getKey(), dataEntry.getValue());
        }
        indexWriter.close();
        outputStream.close();

        // read
        FSDataInputStream inputStream = HadoopUtil.getWorkingFileSystem().open(indexPath);
        ColumnIndexReader indexReader = new ColumnIndexReader(inputStream);
        assertEquals(100, indexReader.getNumberOfRows());
        for (Map.Entry<ByteArray, Integer> dataEntry : data.entrySet()) {
            long t0 = System.currentTimeMillis();
            int row = indexReader.getRows(dataEntry.getKey()).toArray()[0];

            int row1 = indexReader.lookupGtIndex(dataEntry.getKey()).toArray()[0];
            int row2 = indexReader.lookupLtIndex(dataEntry.getKey()).toArray()[0];

            assertEquals(dataEntry.getValue().intValue(), row);
        }
        indexReader.close();
    }
}
