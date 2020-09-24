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
package io.kyligence.kap.source.jdbc;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class JdbcSourceTest extends JdbcTestBase {

    @Test
    public void testBasic() throws IOException {
        ISource source = SourceFactory.getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return ISourceAware.ID_JDBC;
            }

            @Override
            public KylinConfig getConfig() {
                return getTestConfig();
            }
        });
        ISourceMetadataExplorer metadataExplorer = source.getSourceMetadataExplorer();
        ISampleDataDeployer sampleDataDeployer = source.getSampleDataDeployer();
        Assert.assertTrue(source instanceof JdbcSource);
        Assert.assertTrue(metadataExplorer instanceof JdbcExplorer);
        Assert.assertTrue(sampleDataDeployer instanceof JdbcExplorer);

        NSparkCubingEngine.NSparkCubingSource cubingSource = source
                .adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class);
        Assert.assertTrue(cubingSource instanceof JdbcSourceInput);

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc tableDesc = tableMgr.getTableDesc("SSB.PART");
        IReadableTable readableTable = source.createReadableTable(tableDesc);
        Assert.assertTrue(readableTable instanceof JdbcTable);
        IReadableTable.TableReader reader = readableTable.getReader();
        Assert.assertTrue(reader instanceof JdbcTableReader);
        Assert.assertTrue(readableTable.exists());
        Assert.assertNotNull(readableTable.getSignature());
        while(reader.next()){
            String[] row = reader.getRow();
            Assert.assertNotNull(row);
        }
        reader.close();

        SegmentRange segmentRange = source.getSegmentRange("0", "21423423");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange.getStart().equals(0L) && segmentRange.getEnd().equals(21423423L));
        SegmentRange segmentRange2 = source.getSegmentRange("", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
        source.close();
    }
}
