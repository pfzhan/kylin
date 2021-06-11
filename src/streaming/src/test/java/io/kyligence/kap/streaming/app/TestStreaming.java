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

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.source.kafka.NSparkKafkaSource;
import io.kyligence.kap.source.kafka.util.KafkaClient;
import io.kyligence.kap.streaming.CreateStreamingFlatTable;
import io.kyligence.kap.streaming.jobs.StreamingDFMergeJobTest;
import io.kyligence.kap.streaming.jobs.impl.StreamingJobLauncher;
import io.kyligence.kap.streaming.jobs.thread.StreamingJobRunner;
import io.kyligence.kap.streaming.util.AwaitUtils;
import io.kyligence.kap.streaming.util.StreamingTestCase;
import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.utils.EmbededServer;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStreaming extends StreamingTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StreamingDFMergeJobTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
    EmbededServer server;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        server = new EmbededServer();
        server.setup();
        val topic = "ssb-topic";
        server.createTopic(topic, 3);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
        server.teardown();
    }

    @Test
    public void testCreateRepeatEphemeralPath() {
        val config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.zookeeper-connect-string", server.zkAddress());
        config.setProperty("kylin.env.zookeeper-max-retries", "1");

        try {
            ZKUtil.createEphemeralPath(
                    "/kylin/streaming/jobs/test_query" + "_" + StreamingUtils
                            .getJobId("e78a89dd-847f-4574-8afa-8768b4228b72", JobTypeEnum.STREAMING_BUILD.name()),
                    config);

            ZKUtil.createEphemeralPath(
                    "/kylin/streaming/jobs/test_query" + "_" + StreamingUtils
                            .getJobId("e78a89dd-847f-4574-8afa-8768b4228b72", JobTypeEnum.STREAMING_BUILD.name()),
                    config);
        } catch (Exception e) {
            assert (e instanceof KeeperException.NodeExistsException);
        }
    }

    @Test
    public void testKafkaPartitions() {
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

        val dataset = flatTable.generateStreamingDataset(true, 5000, 100);
        val model = flatTableDesc.getDataModel();
        val tableDesc = model.getRootFactTable().getTableDesc();
        val kafkaParam = tableDesc.getKafkaConfig().getKafkaParam();
        Assert.assertEquals(3, KafkaClient.getPartitions(kafkaParam));
        Assert.assertEquals(3, source.getPartitions(kafkaParam));
        Assert.assertEquals("1500", kafkaParam.get("maxOffsetsPerTrigger"));
    }

    /**
     * test StreamingJobRunner class 's  run method
     */
    @Test
    public void testBuildJobRunner_run() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val runner = new StreamingJobRunner(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        runner.init();
        val ss = createSparkSession();
        Assert.assertNotNull(ss);
        AwaitUtils.await(() -> runner.run(), 2000, () -> {
            ss.stop();
        });
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testBuildJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
        val ss = createSparkSession();
        Assert.assertNotNull(ss);
        AwaitUtils.await(() -> launcher.launch(), 2000, () -> {
            ss.stop();
        });
    }

    /**
     * test StreamingJobLauncher class 's  launch method
     */
    @Test
    public void testMergeJobLauncher_launch() {
        val modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val launcher = new StreamingJobLauncher();
        launcher.init(PROJECT, modelId, JobTypeEnum.STREAMING_MERGE);
        val ss = createSparkSession();
        AwaitUtils.await(() -> launcher.launch(), 2000, () -> {
            ss.stop();
        });
    }
}
