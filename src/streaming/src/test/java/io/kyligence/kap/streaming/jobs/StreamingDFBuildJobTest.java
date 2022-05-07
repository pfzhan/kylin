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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.StreamingTestConstant;
import io.kyligence.kap.engine.spark.job.BuildLayoutWithUpdate;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.common.BuildJobEntry;
import io.kyligence.kap.streaming.rest.RestSupport;
import io.kyligence.kap.streaming.util.ReflectionUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;

import static io.kyligence.kap.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

public class StreamingDFBuildJobTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "4965c827-fbb4-4ea1-a744-3f341a3b030d";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testStreamingBuild() {
        val config = getTestConfig();
        KylinBuildEnv.getOrCreate(config);
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val mgr = NDataflowManager.getInstance(config, PROJECT);
        var df = mgr.getDataflow(DATAFLOW_ID);

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        df = mgr.getDataflow(df.getId());

        val layoutEntitys = StreamingUtils.getToBuildLayouts(df);
        var nSpanningTree = NSpanningTreeFactory.fromLayouts(layoutEntitys, DATAFLOW_ID);
        val model = df.getModel();
        val builder = Mockito.spy(new StreamingDFBuildJob(PROJECT));
        val streamingEntry = new StreamingEntry();
        streamingEntry.parseParams(new String[] { PROJECT, DATAFLOW_ID, "3000", "", "xx", DEFAULT_PARSER_NAME });
        val ss = createSparkSession();
        streamingEntry.setSparkSession(ss);

        val tuple3 = streamingEntry.generateStreamQueryForOneModel();
        val batchDF = tuple3._1();
        val streamFlatTable = tuple3._3();

        val seg1 = mgr.appendSegmentForStreaming(df, new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 10L,
                createKafkaPartitionsOffset(3, 100L), createKafkaPartitionsOffset(3, 200L)));
        seg1.setStatus(SegmentStatusEnum.READY);
        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToUpdateSegs(seg1);
        List<NDataLayout> layouts = Lists.newArrayList();
        val dfCopy = df;
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.getIndexPlan(DATAFLOW_ID).getAllLayouts().forEach(layout -> {
            layouts.add(NDataLayout.newDataLayout(dfCopy, seg1.getId(), layout.getId()));
        });
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        mgr.updateDataflow(update2);
        streamFlatTable.seg_$eq(seg1);
        val encodedStreamDataset = streamFlatTable.encodeStreamingDataset(seg1, model, batchDF);
        val batchBuildJob = new BuildJobEntry(ss, PROJECT, DATAFLOW_ID, 100L, seg1, encodedStreamDataset,
                nSpanningTree);
        try {
            val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
            var newDataflow = dfMgr.getDataflow(batchBuildJob.dataflowId());
            Assert.assertEquals(RealizationStatusEnum.OFFLINE, newDataflow.getStatus());
            Assert.assertEquals(4, newDataflow.getSegment(seg1.getId()).getLayoutsMap().size());
            val oldFileCount = newDataflow.getSegment(seg1.getId()).getStorageFileCount();
            val oldByteSize = newDataflow.getSegment(seg1.getId()).getStorageBytesSize();

            builder.streamBuild(batchBuildJob);
            newDataflow = dfMgr.getDataflow(batchBuildJob.dataflowId());
            Assert.assertEquals(RealizationStatusEnum.ONLINE, newDataflow.getStatus());
            Assert.assertEquals(4, newDataflow.getSegment(seg1.getId()).getLayoutsMap().size());
            Assert.assertTrue(newDataflow.getSegment(seg1.getId()).getStorageFileCount() > oldFileCount);
            Assert.assertTrue(newDataflow.getSegment(seg1.getId()).getStorageBytesSize() > oldByteSize);

            dfMgr.updateDataflow(batchBuildJob.dataflowId(), updater -> {
                updater.setStatus(RealizationStatusEnum.OFFLINE);
            });
            Mockito.when(builder.createRestSupport()).thenReturn(new RestSupport(config) {
                public RestResponse execute(HttpRequestBase httpReqBase, Object param) {
                    dfMgr.updateDataflow(batchBuildJob.dataflowId(), updater -> {
                        updater.setStatus(RealizationStatusEnum.ONLINE);
                    });
                    return RestResponse.ok();
                }
            });
            builder.updateSegment(batchBuildJob);
            newDataflow = dfMgr.getDataflow(batchBuildJob.dataflowId());
            Assert.assertEquals(RealizationStatusEnum.ONLINE, newDataflow.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetSegment() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val mgr = NDataflowManager.getInstance(config, PROJECT);
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        var df = mgr.getDataflow(dataflowId);
        val builder = new StreamingDFBuildJob(PROJECT);
        builder.setParam(NBatchConstants.P_DATAFLOW_ID, dataflowId);
        val seg = builder.getSegment("c380dd2a-43b8-4268-b73d-2a5f76236632");
        Assert.assertNotNull(seg);
        Assert.assertEquals("c380dd2a-43b8-4268-b73d-2a5f76236632", seg.getId());
    }

    @Test
    public void testShutdown() {
        StreamingDFBuildJob builder = new StreamingDFBuildJob(PROJECT);
        builder.shutdown();
        BuildLayoutWithUpdate buildLayout = (BuildLayoutWithUpdate) ReflectionUtils.getField(builder,
                "buildLayoutWithUpdate");
        val config = getTestConfig();
        try {
            buildLayout.submit(new BuildLayoutWithUpdate.JobEntity() {
                @Override
                public long getIndexId() {
                    return 0;
                }

                @Override
                public String getName() {
                    return null;
                }

                @Override
                public List<NDataLayout> build() throws IOException {
                    return null;
                }
            }, config);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RejectedExecutionException);
        }
    }
}