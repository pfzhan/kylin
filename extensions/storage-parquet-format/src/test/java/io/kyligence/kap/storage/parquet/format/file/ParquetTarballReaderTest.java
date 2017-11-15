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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ParquetTarballReaderTest extends AbstractParquetFormatTest {
    private Path tarballPath = null;

    public ParquetTarballReaderTest() throws IOException {
        super();
        tarballPath = new Path(qualify("./a.parquettar"));
    }

    @After
    public void cleanup() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(tarballPath)) {
            fs.delete(tarballPath, true);
        }
        super.cleanup();
    }

    @Test
    public void testTarballReader() throws Exception {
        writeRows(groupSize);
        appendFile(100);

        ParquetRawReader reader = new ParquetRawReader(new Configuration(), tarballPath, null, null, 100);
        for (int j = 0; j < ParquetConfig.PagesPerGroup; ++j) {
            GeneralValuesReader valuesReader = reader.getValuesReader(j, 0);
            for (int i = 0; i < ParquetConfig.RowsPerPage; ++i) {
                Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2, 3 });
            }
            Assert.assertNull(valuesReader.readBytes());
        }

        Assert.assertNull(reader.getValuesReader(ParquetConfig.PagesPerGroup, 0));
    }

    private void appendFile(long length) throws IOException {
        byte[] content = new byte[(int) length - 8];
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FSDataOutputStream outs = fs.create(tarballPath);
        outs.writeLong(length);
        outs.write(content);

        ContentSummary cSummary = fs.getContentSummary(path);
        byte[] parquetBytes = new byte[(int) cSummary.getLength()];
        FSDataInputStream ins = fs.open(path);
        ins.read(parquetBytes);
        ins.close();

        outs.write(parquetBytes);
        outs.close();
    }
}
