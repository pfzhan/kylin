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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ParquetPageIndexSpliceWriteReadTest {


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
    public void testReadWrite() throws IOException, ClassNotFoundException {
        File indexFile = File.createTempFile("local", "inv");
        indexFile.deleteOnExit();
        FSDataOutputStream outputStream = new FSDataOutputStream(new FileOutputStream(indexFile));
        ParquetPageIndexSpliceWriter writer = new ParquetPageIndexSpliceWriter(outputStream);
        writer.startDiv(0, new String[]{"name"}, new int[]{1}, new int[]{1}, new boolean[]{true});
        writer.write(new byte[]{0}, 1);
        writer.endDiv();
        writer.startDiv(1, new String[]{"name"}, new int[]{1}, new int[]{1}, new boolean[]{true});
        writer.write(new byte[]{1}, 0);
        writer.endDiv();
        writer.startDiv(0, new String[]{"name"}, new int[]{1}, new int[]{1}, new boolean[]{true});
        writer.write(new byte[]{2}, 2);
        writer.endDiv();
        writer.startDiv(1, new String[]{"name"}, new int[]{1}, new int[]{1}, new boolean[]{true});
        writer.write(new byte[]{3}, 0, 1);
        writer.endDiv();
        writer.close();

        FSDataInputStream inputStream = HadoopUtil.getWorkingFileSystem().open(new Path(indexFile.getAbsolutePath()));
        ParquetPageIndexSpliceReader reader = new ParquetPageIndexSpliceReader(inputStream, indexFile.length(), 0);
        List<ParquetPageIndexReader> indexReaders = reader.getIndexReaderByCuboid(0L);
        Assert.assertEquals(indexReaders.size(), 2);
        Assert.assertTrue(reader.getFullBitmap(0L).contains(1));
        Assert.assertTrue(reader.getFullBitmap(0L).contains(2));
        Assert.assertFalse(reader.getFullBitmap(0L).contains(0));
        inputStream.close();
    }
}
