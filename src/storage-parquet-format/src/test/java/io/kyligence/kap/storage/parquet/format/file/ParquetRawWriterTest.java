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
import org.apache.kylin.common.util.MemoryBudgetController;
import org.junit.Assert;
import org.junit.Test;

public class ParquetRawWriterTest extends AbstractParquetFormatTest {
    public ParquetRawWriterTest() throws IOException {
        super();
    }

    @Test
    public void testWriteAndReadPage() throws Exception {
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path)
                .setType(type).build();
        for (int i = 0; i < ParquetConfig.RowsPerPage * ParquetConfig.PagesPerGroup * 2; ++i) {
            writer.writeRow(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 4, 5 }, new int[] { 1, 1 });
            if ((i % ParquetConfig.RowsPerPage) == 0) {
                Assert.assertEquals(i / ParquetConfig.RowsPerPage, writer.getPageCntSoFar());
            }
        }
        writer.close();
    }

    @Test
    public void testFlushAndReadPage() throws Exception {
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path)
                .setType(type).build();
        for (int i = 0; i < 100; ++i) {
            writer.writeRow(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 4, 5 }, new int[] { 1, 1 });
            writer.flush();
            Assert.assertEquals(i + 1, writer.getPageCntSoFar());
        }

        writer.close();
    }

    @Test
    public void testBelowThresholdMemory() throws Exception {
        int currentMB = MemoryBudgetController.getSystemAvailMB();
        float ratio = 1.0f / currentMB;

        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path)
                .setType(type).setPagesPerGroup(200).setThresholdMemory(currentMB * 2).setMemoryCheckRatio(ratio)
                .build();
        for (int i = 0; i < MemoryBudgetController.ONE_MB; ++i) {
            writer.writeRow(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 4, 5 }, new int[] { 1, 1 });
        }

        Assert.assertTrue(writer.getGroupCntSoFar() > 0);
        writer.close();
    }
}
