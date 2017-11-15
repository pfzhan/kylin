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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.ByteArray;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ParquetBundleReaderTest extends AbstractParquetFormatTest {
    public ParquetBundleReaderTest() throws IOException {
        super();
    }

    @Test
    public void testReadInBundle() throws Exception {
        writeRows(groupSize - 1);

        ImmutableRoaringBitmap bitset = Utils.createBitset(10);
        bitset = deserialize(serialize(bitset));

        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(new Configuration()).setPageBitset(bitset).build();
        List<Object> data = bundleReader.read();
        Assert.assertNotNull(data);
        Assert.assertArrayEquals(((Binary) data.get(0)).getBytes(), new byte[] { 2, 3 });

        for (int i = 0; i < (10 * ParquetConfig.RowsPerPage - 1); ++i) {
            bundleReader.read();
        }

        Assert.assertNull(bundleReader.read());
        bundleReader.close();
    }

    @Test
    public void testReadByteArrayInBundle() throws Exception {
        writeRows(groupSize, true);

        ImmutableRoaringBitmap bitset = Utils.createBitset(3);
        bitset = deserialize(serialize(bitset));

        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(new Configuration()).setPageBitset(bitset).build();
        ByteArray[] row;
        for (int i = 0; i < 3 * ParquetConfig.RowsPerPage; ++i) {
            row = bundleReader.readByteArray();
            Assert.assertArrayEquals(new Integer(i).toString().getBytes(), row[0].toBytes());
        }

        Assert.assertNull(bundleReader.readByteArray());
        bundleReader.close();
    }

    private String serialize(ImmutableRoaringBitmap bitset) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitset.serialize(dos);
        dos.close();

        return new String(bos.toByteArray());
    }

    private ImmutableRoaringBitmap deserialize(String str) {
        return new ImmutableRoaringBitmap(ByteBuffer.wrap(str.getBytes()));
    }
}
