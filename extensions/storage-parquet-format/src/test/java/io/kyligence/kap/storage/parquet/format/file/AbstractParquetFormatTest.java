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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public abstract class AbstractParquetFormatTest extends LocalFileMetadataTestCase {
    protected Path path, indexPath;
    protected static String tempFilePath;
    protected int groupSize = ParquetConfig.PagesPerGroup * ParquetConfig.RowsPerPage;
    protected MessageType type;

    public AbstractParquetFormatTest() throws IOException {
        path = new Path("./a.parquet");
        indexPath = new Path("./a.parquetindex");
        cleanTestFile(path);
        type = new MessageType("test", new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 2, "key1"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m1"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m2"));
    }

    @After
    public void cleanup() throws IOException {
        cleanTestFile(path);
        cleanAfterClass();
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    protected void writeRows(int rowCnt) throws Exception {
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path).setType(type).build();
        for (int i = 0; i < rowCnt; ++i) {
            writer.writeRow(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 4, 5 }, new int[] { 1, 1 });
        }
        writer.close();
    }

    protected void cleanTestFile(Path path) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        if (fs.exists(indexPath)) {
            fs.delete(indexPath, true);
        }
    }
}
