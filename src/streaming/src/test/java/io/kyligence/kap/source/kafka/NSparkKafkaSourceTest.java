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
package io.kyligence.kap.source.kafka;

import static io.kyligence.kap.metadata.model.NTableMetadataManager.getInstance;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.source.SourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.StreamingTestConstant;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;

public class NSparkKafkaSourceTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetSourceMetadataExplorer() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        val kafkaExplorer = source.getSourceMetadataExplorer();
        Assert.assertTrue(kafkaExplorer instanceof KafkaExplorer);
        Assert.assertTrue(kafkaExplorer.checkTablesAccess(Collections.emptySet()));
    }

    @Test
    public void testAdaptToBuildEngine() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        Assert.assertFalse(source.enableMemoryStream());
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val ss = createSparkSession();
        val tableMetadataManager = getInstance(getTestConfig(), PROJECT);
        val tableDesc = tableMetadataManager.getTableDesc("SSB.P_LINEORDER");
        var engineAdapter = SourceFactory.createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class);
        var ds = engineAdapter.getSourceData(tableDesc, ss, new HashMap<>());
        Assert.assertEquals(1, ds.count());

        thrown.expect(UnsupportedOperationException.class);
        source.createReadableTable(tableDesc);
    }

    @Test
    public void testAdaptToBuildEngine1() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(true);
        Assert.assertTrue(source.enableMemoryStream());
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val tableMetadataManager = getInstance(getTestConfig(), PROJECT);
        val tableDesc = tableMetadataManager.getTableDesc("SSB.P_LINEORDER");

        val engineAdapter = SourceFactory.createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class);
        val ss = createSparkSession();
        var ds = engineAdapter.getSourceData(tableDesc, ss, new HashMap<>());

        Assert.assertEquals("memory", ds.logicalPlan().toString());
    }

    @Test
    public void testCreateReadableTable() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        thrown.expect(UnsupportedOperationException.class);
        source.createReadableTable(null);
    }

    @Test
    public void testEnrichSourcePartitionBeforeBuild() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        thrown.expect(UnsupportedOperationException.class);
        source.enrichSourcePartitionBeforeBuild(null, null);
    }

    @Test
    public void testGetSampleDataDeployer() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        thrown.expect(UnsupportedOperationException.class);
        source.getSampleDataDeployer();
    }

    @Test
    public void testGetSegmentRange() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        val seg = (SegmentRange.KafkaOffsetPartitionedSegmentRange) source.getSegmentRange("1234", "5678");
        Assert.assertTrue(seg instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange);
        Assert.assertEquals(1234L, seg.getStart().longValue());
        Assert.assertEquals(5678L, seg.getEnd().longValue());

        val seg1 = (SegmentRange.KafkaOffsetPartitionedSegmentRange) source.getSegmentRange("", "");
        Assert.assertEquals(0L, seg1.getStart().longValue());
        Assert.assertEquals(Long.MAX_VALUE, seg1.getEnd().longValue());
    }

    @Test
    public void testSupportBuildSnapShotByPartition() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        Assert.assertTrue(source.supportBuildSnapShotByPartition());
    }
}
