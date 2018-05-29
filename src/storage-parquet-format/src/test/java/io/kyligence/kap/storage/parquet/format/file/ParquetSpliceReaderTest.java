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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ParquetSpliceReaderTest extends AbstractParquetFormatTest {
    public ParquetSpliceReaderTest() throws IOException {
        super();
    }

    @Test
    public void testGetDiv() throws Exception {
        Configuration conf = new Configuration();
        ParquetSpliceWriter writer = new ParquetSpliceWriter.Builder().setConf(conf).setPath(path).setType(type).build();
        for (int i = 0; i < ParquetConfig.PagesPerGroup; ++i) {
            writer.startDiv(String.valueOf(i));
            writer.writeRow(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 4, 5 }, new int[] { 1, 1 });
            writer.endDiv();
        }
        writer.close();

        ParquetSpliceReader reader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path).build();
        Assert.assertEquals(ParquetConfig.PagesPerGroup, reader.getDivs().size());
        for (int i = 0; i < ParquetConfig.PagesPerGroup; ++i) {
            Assert.assertTrue(reader.getDivs().contains(String.valueOf(i)));
        }
    }

    @Test
    public void testReadDiv() throws Exception {
        Configuration conf = new Configuration();
        ParquetSpliceWriter writer = new ParquetSpliceWriter.Builder().setConf(conf).setPath(path).setType(type).build();
        for (int i = 0; i < ParquetConfig.PagesPerGroup; ++i) {
            writer.startDiv(String.valueOf(i));
            List<Object> row = Lists.newArrayList();
            row.add(Binary.fromConstantByteArray(new Integer(i).toString().getBytes()));
            row.add(Binary.fromConstantByteArray(new byte[] {1}));
            row.add(Binary.fromConstantByteArray(new byte[] {2}));
            writer.writeRow(row);
            writer.endDiv();
        }
        writer.close();

        ParquetSpliceReader reader = new ParquetSpliceReader.Builder().setConf(conf).setPath(path).build();
        ParquetBundleReader bundleReader = reader.getDivReader(String.valueOf(ParquetConfig.PagesPerGroup / 2));
        List<Object> data = bundleReader.read();
        Assert.assertEquals(ParquetConfig.PagesPerGroup / 2, bundleReader.getPageIndex());
        Assert.assertArrayEquals(new Integer(ParquetConfig.PagesPerGroup / 2).toString().getBytes(), ((Binary)data.get(0)).getBytes());
        Assert.assertNull(bundleReader.read());
    }
}
