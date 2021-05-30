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
package io.kyligence.kap.rest.service;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;

import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.streaming.RDBMSStreamingJobStatsDAO;
import io.kyligence.kap.metadata.streaming.StreamingJobStats;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.UUID;

public class StreamingJobServiceTest extends CSVSourceTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private StreamingJobService streamingJobService = Mockito.spy(new StreamingJobService());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    private static String[] timeZones = { "GMT+8", "CST", "PST", "UTC" };

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    private static String DATAFLOW_ID = MODEL_ID;

    @Before
    public void setup() {
        super.setup();
        System.setProperty("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(streamingJobService, "aclEvaluate", aclEvaluate);

        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject(PROJECT);
        val copy = prjManager.copyForWrite(prj);
        copy.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        prjManager.updateProject(copy);
        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }

        EventBusFactory.getInstance().register(modelBrokenListener, false);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateStreamingJobStatusToStart() throws Exception {
        streamingJobService.updateStreamingJobStatus(PROJECT, MODEL_ID, "START");
        KylinConfig testConfig = getTestConfig();
        StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(testConfig, PROJECT);
        String buildJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val buildMeta = streamingJobManager.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildMeta.getCurrentStatus());

        String mergeJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name());
        val mergeMeta = streamingJobManager.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeMeta.getCurrentStatus());
    }

    @Test
    public void testUpdateStreamingJobStatusToStop() throws Exception {
        streamingJobService.updateStreamingJobStatus(PROJECT, MODEL_ID, "START");
        streamingJobService.updateStreamingJobStatus(PROJECT, MODEL_ID, "STOP");
        KylinConfig testConfig = getTestConfig();

        StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(testConfig, PROJECT);
        String buildJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val buildMeta = streamingJobManager.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildMeta.getCurrentStatus());

        String mergeJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name());
        val mergeMeta = streamingJobManager.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeMeta.getCurrentStatus());
    }

    @Test
    public void testUpdateStreamingJobParams() throws Exception {
        val buildParam = new HashMap<String, String>();
        val mergeParam = new HashMap<String, String>();

        buildParam.put("spark.executor.memory", "2g");
        buildParam.put("spark.master", "yarn");
        buildParam.put("spark.driver.memory", "1g");
        buildParam.put("kylin.streaming.duration", "60");
        buildParam.put("spark.executor.cores", "1");
        buildParam.put("spark.executor.instances", "5");
        buildParam.put("kylin.streaming.job-retry-enabled", "true");
        buildParam.put("spark.sql.shuffle.partitions", "10");

        mergeParam.put("spark.executor.memory", "3g");
        mergeParam.put("spark.master", "yarn");
        mergeParam.put("spark.driver.memory", "3g");
        mergeParam.put("kylin.streaming.segment-merge-threshold", "5");
        mergeParam.put("spark.executor.cores", "3");
        mergeParam.put("spark.executor.instances", "6");
        buildParam.put("kylin.streaming.job-retry-enabled", "true");
        mergeParam.put("spark.sql.shuffle.partitions", "20");
        streamingJobService.updateStreamingJobParams(PROJECT, MODEL_ID, buildParam, mergeParam);
        KylinConfig testConfig = getTestConfig();
        StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(testConfig, PROJECT);
        String buildJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val buildMeta = streamingJobManager.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(buildParam.toString(), buildMeta.getParams().toString());

        String mergeJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name());
        val mergeMeta = streamingJobManager.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(mergeParam.toString(), mergeMeta.getParams().toString());
    }

    @Test
    public void testUpdateStreamingJobInfo() throws Exception {
        val req = new StreamingJobUpdateRequest();
        req.setProject(PROJECT);
        req.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        req.setModelId(MODEL_ID);
        req.setNodeInfo("10.3.1.68:7070");
        req.setProcessId("9876");
        req.setYarnAppUrl("http://spark1:8088/proxy/application_1616466883257_1384/");
        req.setYarnAppId("application_1616466883257_1384");
        streamingJobService.updateStreamingJobInfo(req);
        KylinConfig testConfig = getTestConfig();
        StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(testConfig, PROJECT);
        String buildJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val meta = streamingJobManager.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals("10.3.1.68:7070", meta.getNodeInfo());
        Assert.assertEquals("9876", meta.getProcessId());
        Assert.assertEquals("http://spark1:8088/proxy/application_1616466883257_1384/", meta.getYarnAppUrl());
        Assert.assertEquals("application_1616466883257_1384", meta.getYarnAppId());
        Assert.assertNotNull(meta.getLastUpdateTime());
    }

    @Test
    public void testAddSegmentForMerge() throws Exception {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957110000L, 1613957130000L,
                createKafkaPartitionsOffset(3, 100L), createKafkaPartitionsOffset(3, 300L));
        val newSegId = UUID.randomUUID().toString();
        streamingJobService.addSegment(PROJECT, DATAFLOW_ID, rangeToMerge, "0", newSegId);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val newSeg = df.getSegment(newSegId);
        Assert.assertEquals(newSegId, newSeg.getId());
        Assert.assertEquals("1", newSeg.getAdditionalInfo().get(StreamingConstants.FILE_LAYER));
    }

    @Test
    public void testAppendSegment() throws Exception {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957140000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 400L), createKafkaPartitionsOffset(3, 500L));
        val newSegId = UUID.randomUUID().toString();
        streamingJobService.addSegment(PROJECT, DATAFLOW_ID, rangeToMerge, null, newSegId);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val newSeg = df.getSegment(newSegId);
        Assert.assertEquals(newSegId, newSeg.getId());
        Assert.assertNull(newSeg.getAdditionalInfo().get(StreamingConstants.FILE_LAYER));
    }

    @Test
    public void testUpdateSegmentForOnline() throws Exception {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236901";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        streamingJobService.updateSegment(PROJECT, dataflowId, segId, null, "ONLINE");
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);

        Assert.assertEquals(SegmentStatusEnum.READY, seg.getStatus());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
    }

    @Test
    public void testDeleteSegment() throws Exception {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236901";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);
        Assert.assertNotNull(seg);
        streamingJobService.deleteSegment(PROJECT, dataflowId, Arrays.asList(seg));
        NDataflowManager mgr1 = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df1 = mgr1.getDataflow(dataflowId);
        val seg1 = df1.getSegment(segId);
        Assert.assertNull(seg1);
    }

    @Test
    public void testUpdateLayout() throws Exception {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236633";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);
        Assert.assertEquals(17, seg.getLayoutSize());
        val layouts = new ArrayList<NDataLayout>();
        layouts.add(NDataLayout.newDataLayout(df, seg.getId(), 10002L));
        streamingJobService.updateLayout(PROJECT, DATAFLOW_ID, layouts);
        NDataflowManager mgr1 = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df1 = mgr1.getDataflow(dataflowId);
        val seg1 = df1.getSegment(segId);
        Assert.assertEquals(18, seg1.getLayoutSize());
    }

    @Test
    public void testCollectStreamingJobStats() {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        streamingJobService.collectStreamingJobStats(jobId, PROJECT, 123L, 123.2, 42L, 50L);
        KylinConfig testConfig = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(testConfig, PROJECT);
        StreamingJobMeta jobMeta = mgr.getStreamingJobByUuid(jobId);
        String lastUpdateTime = jobMeta.getLastUpdateTime();
        int lastBatchCount = jobMeta.getLastBatchCount();
        Assert.assertNotNull(lastUpdateTime);
        Assert.assertEquals(123, lastBatchCount);
    }

    @Test
    public void testGetStreamingJobInfoOfNoData() {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val resp = streamingJobService.getStreamingJobInfo(MODEL_ID, PROJECT);
        Assert.assertEquals(JobStatusEnum.STOPPED, resp.getBuildJobMeta().getCurrentStatus());
        KylinConfig config = getTestConfig();

        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });

        val resp1 = streamingJobService.getStreamingJobInfo(MODEL_ID, PROJECT);
        Assert.assertEquals(JobStatusEnum.RUNNING, resp1.getBuildJobMeta().getCurrentStatus());
        Assert.assertEquals(0, resp1.getAvgConsumeRateIn5mins().intValue());
        Assert.assertEquals(0, resp1.getAvgConsumeRateIn15mins().intValue());
        Assert.assertEquals(0, resp1.getAvgConsumeRateIn30mins().intValue());
        Assert.assertEquals(0, resp1.getAvgConsumeRateInAll().intValue());
        Assert.assertEquals(0, resp1.getLatency());

        Assert.assertNotNull(resp1.getLastBuildTime());
        Assert.assertNotNull(resp1.getLastUpdateTime());
    }

    @Test
    public void testGetStreamingJobInfo() {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        createData(jobId);
        val resp = streamingJobService.getStreamingJobInfo(MODEL_ID, PROJECT);
        Assert.assertEquals(JobStatusEnum.STOPPED, resp.getBuildJobMeta().getCurrentStatus());
        KylinConfig config = getTestConfig();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(System.currentTimeMillis() - 60 * 60 * 1000);
            copyForWrite.setLastStartTime(format.format(cal.getTime()));
            cal.setTimeInMillis(System.currentTimeMillis());
            copyForWrite.setLastUpdateTime(format.format(cal.getTime()));
        });

        val resp1 = streamingJobService.getStreamingJobInfo(MODEL_ID, PROJECT);
        Assert.assertEquals(JobStatusEnum.RUNNING, resp1.getBuildJobMeta().getCurrentStatus());
        Assert.assertEquals(24, resp1.getAvgConsumeRateIn5mins().intValue());
        Assert.assertEquals(8, resp1.getAvgConsumeRateIn15mins().intValue());
        Assert.assertEquals(4, resp1.getAvgConsumeRateIn30mins().intValue());
        Assert.assertEquals(1, resp1.getAvgConsumeRateInAll().intValue());
        Assert.assertNotNull(resp1.getLatency());

        Assert.assertNotNull(resp1.getLastBuildTime());
        Assert.assertNotNull(resp1.getLastUpdateTime());
    }

    private void createData(String jobId) {
        val config = getTestConfig();
        config.setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");

        val streamingJobsStatsDAO = RDBMSStreamingJobStatsDAO.getInstance();
        val now = System.currentTimeMillis();
        for (int i = 60; i > 0; i--) {
            streamingJobsStatsDAO.insert(new StreamingJobStats(jobId, PROJECT, 120L, 32.22, 60000L, now - i * 1000));
        }
    }

    @Test
    public void testForceStopStreamingJob() {
        val buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";

        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(buildJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        mgr.updateStreamingJob(mergeJobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        streamingJobService.forceStopStreamingJob(PROJECT, MODEL_ID);
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeJobMeta.getCurrentStatus());
    }
}
