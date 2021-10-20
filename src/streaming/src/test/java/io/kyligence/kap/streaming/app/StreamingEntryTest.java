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
package io.kyligence.kap.streaming.app;

import java.io.File;
import java.util.HashMap;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
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
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.CreateStreamingFlatTable;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.jobs.StreamingJobUtils;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.rest.RestSupport;
import io.kyligence.kap.streaming.util.AwaitUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingEntryTest extends StreamingTestCase {

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
    public void testBuild() {
        val config = KylinConfig.getInstanceFromEnv();

        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dfMgr = NDataflowManager.getInstance(config, PROJECT);
        var df = dfMgr.getDataflow(DATAFLOW_ID);
        // cleanup all segments first
        var update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        df = dfMgr.getDataflow(df.getId());
        val seg1 = dfMgr.appendSegmentForStreaming(df, createSegmentRange());
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan());
        val layouts = StreamingUtils.getToBuildLayouts(df);
        Assert.assertNotNull(layouts);
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "" };
        val entry = new StreamingEntry(args);
        val nSpanningTree = entry.createSpanningTree(df);
        Assert.assertNotNull(nSpanningTree);

        val ss = createSparkSession();
        val flatTable = CreateStreamingFlatTable.apply(flatTableDesc, null, nSpanningTree, ss, null, null, null);

        val ds = flatTable.generateStreamingDataset(config);
        Assert.assertEquals(1, ds.count());
        var model = flatTableDesc.getDataModel();
        var tableDesc = model.getRootFactTable().getTableDesc();
        var kafkaParam = tableDesc.getKafkaConfig().getKafkaParam();
        Assert.assertEquals("earliest", kafkaParam.get("startingOffsets"));

        val jobParams = new HashMap<String, String>();
        jobParams.put(StreamingConstants.STREAMING_KAFKA_STARTING_OFFSETS, "latest");
        val newConfig = StreamingJobUtils.getStreamingKylinConfig(config, jobParams, model.getId(), PROJECT);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        flatTable.generateStreamingDataset(newConfig);
        model = flatTableDesc.getDataModel();
        tableDesc = model.getRootFactTable().getTableDesc();
        kafkaParam = tableDesc.getKafkaConfig().getKafkaParam();
        Assert.assertEquals("latest", kafkaParam.get("startingOffsets"));
        ss.stop();
    }

    @Test
    public void testMain() {
        val config = getTestConfig();
        clearCheckpoint(DATAFLOW_ID);
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(true);
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "" };
        thrown.expect(Exception.class);
        StreamingEntry.main(args);
    }

    @Test
    public void testExecute() {
        val config = getTestConfig();
        clearCheckpoint(DATAFLOW_ID);
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(true);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());

        val args = new String[] { PROJECT, DATAFLOW_ID, "2", "" };
        val entry = Mockito.spy(new StreamingEntry(args));
        entry.setSparkSession(createSparkSession());
        StreamingEntry.entry_$eq(entry);
        Assert.assertNotNull(StreamingEntry.self());
        Mockito.when(entry.createRestSupport(config)).thenReturn(new RestSupport(config) {
            public RestResponse execute(HttpEntityEnclosingRequestBase httpReqBase, Object param) {
                return RestResponse.ok();
            }
        });

        AwaitUtils.await(() -> {
        }, 5000, () -> {
            val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
            val jobId = DATAFLOW_ID + "_build";
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                copyForWrite.setAction(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN);
            });
        });
        try {
            entry.execute();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        clearCheckpoint(DATAFLOW_ID);
    }

    @Test
    public void testDimensionTableRefresh() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val dataflowId = "511a9163-7888-4a60-aa24-ae735937cc87";
        val args = new String[] { PROJECT, dataflowId, "5", "" };
        val entry = Mockito.spy(new StreamingEntry(args));
        entry.setSparkSession(createSparkSession());
        val tuple4 = entry.generateStreamQueryForOneModel(entry.ss, PROJECT, dataflowId, "");
        entry.rateTriggerDuration_$eq(1000);
        val flatTable = tuple4._3();
        flatTable.tableRefreshInterval_$eq(5L);
        Assert.assertEquals(dataflowId, flatTable.model().getId());
        try {
            entry.startTableRefreshThread(flatTable);
            AwaitUtils.await(() -> {
            }, 10000, () -> {
                entry.refreshTable(flatTable);
                Assert.assertEquals(0L, entry.tableRefreshAcc().get());
                StreamingEntry.entry_$eq(entry);
                StreamingEntry.stop();
                entry.ss.stop();
            });
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private void clearCheckpoint(String dataflowId) {
        val config = getTestConfig();
        val checkpointFile = new File(config.getStreamingBaseCheckpointLocation() + "/" + dataflowId);
        var result = false;
        int retry = 0;
        while (!result && retry < 5 && checkpointFile.exists()) {
            result = checkpointFile.delete();
            StreamingUtils.sleep(5000);
            retry++;
        }
    }
}
