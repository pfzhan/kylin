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
package io.kyligence.kap.streaming.jobs;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.kyligence.kap.common.StreamingTestConstant;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.streaming.CreateStreamingFlatTable;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;

public class CreateStreamingFlatTableTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "511a9163-7888-4a60-aa24-ae735937cc87";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGenerateStreamingDataset() {
        val config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "100");
        config.setProperty("kylin.streaming.kafka-conf.security.protocol", "SASL_PLAINTEXT");
        config.setProperty("kylin.streaming.kafka-conf.sasl.mechanism", "PLAIN");
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());

        val args = new String[] { PROJECT, DATAFLOW_ID, "5", "10 seconds" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        val dfMgr = NDataflowManager.getInstance(config, PROJECT);
        val dataflow = dfMgr.getDataflow(DATAFLOW_ID);
        val flatTableDesc = new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan());

        val seg = NDataSegment.empty();
        seg.setId("test-1234");

        val steamingFlatTable = CreateStreamingFlatTable.apply(flatTableDesc, seg, entry.createSpanningTree(dataflow),
                entry.getSparkSession(), null, "LO_PARTITIONCOLUMN", null);

        val ds = steamingFlatTable.generateStreamingDataset(config);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        Assert.assertEquals(4, steamingFlatTable.lookupTablesGlobal().size());
        Assert.assertEquals(-1, steamingFlatTable.tableRefreshInterval());
        Assert.assertFalse(steamingFlatTable.shouldRefreshTable());
        Assert.assertEquals(DATAFLOW_ID, steamingFlatTable.model().getId());
        Assert.assertNotNull(ds);
        Assert.assertEquals(60, ds.encoder().schema().size());

    }
}
