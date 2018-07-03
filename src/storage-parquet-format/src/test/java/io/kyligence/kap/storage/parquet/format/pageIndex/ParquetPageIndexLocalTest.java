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

import java.io.FileOutputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;
import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

@Ignore("This is used for debugging index.")
public class ParquetPageIndexLocalTest {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexLocalTest.class);

    private final double spillThresholdMB = 128;

    private CleanMetadataHelper cleanMetadataHelper = null;

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();

    }

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @Test
    public void buildIndex() throws Exception {
        //        writeRows(ParquetConfig.RowsPerPage);

        ParquetBundleReader reader = new ParquetBundleReader.Builder()
                .setPath(new Path("/Users/dong/Documents/9.parquet")).setConf(new Configuration()).build();
        String[] name = { "puttime", "key", "uid", "tbl", "fsize", "hash", "md5", "ip", "mimetype" };
        int[] length = { 8, 8, 8, 8, 8, 8, 8, 8, 8 };
        int[] card = { 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        boolean[] onlyEQ = { true, true, true, true, true, true, true, true, true };

        ColumnSpec[] specs = new ColumnSpec[9];
        for (int i = 0; i < 9; i++) {
            specs[i] = new ColumnSpec(name[i], length[i], card[i], onlyEQ[i], i);
            specs[i].setValueEncodingIdentifier('s');
            //            specs[i].setKeyEncodingIdentifier('a');
            //            specs[i].setValueEncodingIdentifier('s');
        }
        ColumnSpec[] specsSub = new ColumnSpec[1];
        specsSub[0] = new ColumnSpec("key", 6, 1, true, 1);
        specsSub[0].setValueEncodingIdentifier('s');
        ParquetPageIndexWriter indexWriter = new ParquetPageIndexWriter(specs,
                new FSDataOutputStream(new FileOutputStream("/tmp/new2.parquetindex")), 0);
        ParquetPageIndexWriter indexWriterSub = new ParquetPageIndexWriter(specsSub,
                new FSDataOutputStream(new FileOutputStream("/tmp/new3.parquetindex")), 0);

        int i = 0;
        while (true) {
            if (i++ % 100000 == 0) {
                System.out.println(i);
            }
            List<Object> readObjects = reader.read();
            if (readObjects == null) {
                break;
            }
            List<byte[]> byteList = Lists.newArrayListWithExpectedSize(readObjects.size());
            for (Object obj : readObjects) {
                byteList.add(((Binary) obj).getBytes());
            }
            byteList = RawTableUtils.hash(byteList);
            int pageIndex = reader.getPageIndex();
            indexWriter.write(byteList, pageIndex);

            writeSubstring(indexWriterSub, byteList.get(1), pageIndex, 6);

            long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
            if (availMemoryMB < spillThresholdMB) {
                logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
                indexWriter.spill();
                indexWriterSub.spill();
                logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
            }
        }

        indexWriter.close();
        indexWriterSub.close();
        reader.close();
    }

    private void writeSubstring(ParquetPageIndexWriter writer, byte[] value, int pageId, int length) {
        // skip if value's length is less than required length
        if (value.length < length) {
            return;
        }

        for (int index = 0; index <= (value.length - length); index++) {
            writer.write(value, index, pageId);
        }
    }

}