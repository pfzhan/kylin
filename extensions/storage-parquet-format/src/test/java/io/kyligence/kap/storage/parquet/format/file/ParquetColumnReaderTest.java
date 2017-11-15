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

import io.kyligence.kap.common.util.ExpandableBytesVector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class ParquetColumnReaderTest extends AbstractParquetFormatTest {

    public ParquetColumnReaderTest() throws IOException {
        super();
    }

    @Test
    public void testGetNextValuesReader() throws Exception {
        writeRows(ParquetConfig.RowsPerPage);

        ParquetRawReader rawReader = new ParquetRawReader(new Configuration(), path, null, null, 0);
        ParquetColumnReader reader = new ParquetColumnReader(rawReader, 0, null);
        GeneralValuesReader valuesReader = reader.getNextValuesReader();
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2, 3 });
        Assert.assertNull(reader.getNextValuesReader());
        rawReader.close();
    }

    @Test
    public void testGetPageIndex() throws Exception {
        writeRows(ParquetConfig.RowsPerPage * ParquetConfig.PagesPerGroup);

        ParquetRawReader rawReader = new ParquetRawReader(new Configuration(), path, null, null, 0);
        ParquetColumnReader reader = new ParquetColumnReader(rawReader, 0, null);
        int count = 0;
        while (true) {
            if (reader.getNextValuesReader() == null) {
                break;
            }
            Assert.assertEquals(count++, reader.getPageIndex());
        }
        rawReader.close();
    }

    @Test
    public void testGetPageIndexWithPageBitmap() throws Exception {
        writeRows(ParquetConfig.RowsPerPage * ParquetConfig.PagesPerGroup);

        ParquetRawReader rawReader = new ParquetRawReader(new Configuration(), path, null, null, 0);
        ParquetColumnReader reader = new ParquetColumnReader(rawReader, 0, Utils.createBitset(1, ParquetConfig.PagesPerGroup - 1));
        int count = 1;
        while (true) {
            if (reader.getNextValuesReader() == null) {
                break;
            }
            Assert.assertEquals(count++, reader.getPageIndex());
        }
        
        rawReader.close();
    }

    @Test
    public void testGetPageValuesReader() throws Exception {
        writeRows(ParquetConfig.RowsPerPage * ParquetConfig.PagesPerGroup);
        
        ParquetRawReader rawReader = new ParquetRawReader(new Configuration(), path, null, null, 0);
        ParquetColumnReader reader = new ParquetColumnReader(rawReader, 0, null);
        ExpandableBytesVector buffer;
        for (int i = 0; i < ParquetConfig.PagesPerGroup; i++) {
            System.out.println("i: " + i);
            buffer = reader.readNextPage();
            Assert.assertNotNull(buffer);
        }
        buffer = reader.readNextPage();
        Assert.assertNull(buffer);
        
        rawReader.close();
    }
}
