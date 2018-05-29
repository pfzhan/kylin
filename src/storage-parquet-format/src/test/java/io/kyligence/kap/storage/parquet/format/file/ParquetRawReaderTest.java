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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.common.util.ExpandableBytesVector;
import io.kyligence.kap.storage.parquet.format.file.pagereader.PageValuesReader;

public class ParquetRawReaderTest extends AbstractParquetFormatTest {

    public ParquetRawReaderTest() throws IOException {
        super();
    }

    @Test
    public void testColumnCount() throws Exception {
        writeRows(ParquetConfig.RowsPerPage);

        ParquetRawReader reader = new ParquetRawReader.Builder().setPath(path).setConf(new Configuration()).build();
        Assert.assertEquals(reader.getColumnCount(), 3);
        reader.close();
    }

    @Test
    public void testReadPageByGlobalPageIndex() throws Exception {
        writeRows(groupSize, false);

        ParquetRawReader reader = new ParquetRawReader.Builder().setPath(path).setConf(new Configuration()).build();
        GeneralValuesReader valuesReader = reader.getValuesReader(ParquetConfig.PagesPerGroup - 1, 0);
        for (int i = 0; i < ParquetConfig.RowsPerPage; ++i) {
            Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new Integer(i + (ParquetConfig.PagesPerGroup - 1) * ParquetConfig.RowsPerPage).toString().getBytes());
        }
        Assert.assertNull(valuesReader.readBytes());
        reader.close();
    }

    @Test
    public void testReadPageByGlobalPageIndexV2() throws Exception {
        writeRows(groupSize, true);

        ParquetRawReader reader = new ParquetRawReader.Builder().setPath(path).setConf(new Configuration()).build();
        GeneralValuesReader valuesReader = reader.getValuesReader(ParquetConfig.PagesPerGroup - 1, 0);
        for (int i = 0; i < ParquetConfig.RowsPerPage; ++i) {
            Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new Integer(i + (ParquetConfig.PagesPerGroup - 1) * ParquetConfig.RowsPerPage).toString().getBytes());
        }
        Assert.assertNull(valuesReader.readBytes());
        reader.close();
    }

    @Test
    public void testReadPageByGroupAndPageIndex() throws Exception {
        writeRows(groupSize, false);
        ParquetRawReader reader = new ParquetRawReader.Builder().setPath(path).setConf(new Configuration()).build();
        GeneralValuesReader valuesReader = reader.getValuesReader(0, 2, 1);
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2 });
        for (int i = 0; i < (ParquetConfig.RowsPerPage - 1); ++i) {
            Assert.assertNotNull(valuesReader.readBytes());
        }
        Assert.assertNull(valuesReader.readBytes());
        reader.close();
    }

    @Test
    public void testReadPageAtOneTime() throws Exception {
        writeRows(groupSize);
        ParquetRawReader reader = new ParquetRawReader.Builder().setPath(path).setConf(new Configuration()).build();
        PageValuesReader pageReader = reader.getPageValuesReader(ParquetConfig.PagesPerGroup - 1, 0);
        ExpandableBytesVector buffer = new ExpandableBytesVector(1000);
        pageReader.readPage(buffer);

        int baseOffset = buffer.getOffset(0);
        
        Assert.assertEquals(ParquetConfig.RowsPerPage, buffer.getRowCount());
        Assert.assertEquals(ParquetConfig.RowsPerPage * 2 + baseOffset, buffer.getTotalLength());
        for (int i = 0; i < ParquetConfig.RowsPerPage; i++) {
            Assert.assertEquals(baseOffset + i * 2, buffer.getOffset(i));
            Assert.assertTrue(((byte) 2) == buffer.getData()[baseOffset + i * 2]);
            Assert.assertTrue(((byte) 3) == buffer.getData()[baseOffset + i * 2 + 1]);
        }
        reader.close();
    }
}
