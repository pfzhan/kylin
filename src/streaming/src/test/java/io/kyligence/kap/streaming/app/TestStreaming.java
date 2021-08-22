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

import java.util.HashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.source.SourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.StreamingTestConstant;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.source.kafka.NSparkKafkaSource;
import io.kyligence.kap.streaming.CreateStreamingFlatTable;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.jobs.StreamingJobUtils;
import io.kyligence.kap.streaming.jobs.impl.StreamingJobLauncher;
import io.kyligence.kap.streaming.jobs.thread.StreamingJobRunner;
import io.kyligence.kap.streaming.util.AwaitUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestStreaming extends StreamingTestCase {

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

        val source = (NSparkKafkaSource) SourceFactory.getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return 1;
            }

            @Override
            public KylinConfig getConfig() {
                return config;
            }
        });
        assert source.supportBuildSnapShotByPartition();
        source.enableMemoryStream(true);
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
        val nSpanningTree = NSpanningTreeFactory.fromLayouts(layouts, DATAFLOW_ID);

        val ss = createSparkSession();
        val flatTable = CreateStreamingFlatTable.apply(flatTableDesc, null, nSpanningTree, ss, null, null, null);

        flatTable.generateStreamingDataset(config);
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

    /**
     * test StreamingJobRunner class 's  run method
     */
    @Test
    public void testBuildJobRunner_run() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.init();
        AwaitUtils.await(() -> runner.run(), 10000, () -> {});
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testBuildJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        AwaitUtils.await(() -> launcher.launch(), 10000, () -> {});
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testMergeJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        AwaitUtils.await(() -> launcher.launch(), 10000, () -> {});
    }
}
