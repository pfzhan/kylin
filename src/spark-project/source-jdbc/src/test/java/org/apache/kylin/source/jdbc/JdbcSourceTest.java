/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.source.jdbc;

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
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.junit.Assert;
import org.junit.Test;

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
        while (reader.next()) {
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
        assert !source.supportBuildSnapShotByPartition();
        source.close();
    }
}
