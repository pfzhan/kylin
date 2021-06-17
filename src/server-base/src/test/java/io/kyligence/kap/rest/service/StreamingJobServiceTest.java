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
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;

import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.streaming.StreamingJobStatsManager;
import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.metadata.streaming.StreamingJobRecordManager;
import io.kyligence.kap.metadata.streaming.StreamingJobStats;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.request.StreamingJobFilter;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import lombok.val;
import lombok.var;
import org.apache.commons.lang3.StringUtils;
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
import org.springframework.beans.BeanUtils;
import org.springframework.test.util.ReflectionTestUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
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

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

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
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(streamingJobService, "modelService", modelService);
        ReflectionTestUtils.setField(streamingJobService, "indexPlanService", indexPlanService);

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
    public void testGetStreamingJobList() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val streamingJobsStatsManager = createStatData(jobId);

        var jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        var list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(8, list.getTotalSize());

        // modelName filter
        jobFilter = new StreamingJobFilter("stream_merge", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(6, list.getTotalSize());

        jobFilter = new StreamingJobFilter("stream_merge1", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(2, list.getTotalSize());

        jobFilter = new StreamingJobFilter("stream_merge2", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(0, list.getTotalSize());

        jobFilter = new StreamingJobFilter("", Arrays.asList("stream_merge1"), Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(2, list.getTotalSize());

        // job types filter
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Arrays.asList("STREAMING_BUILD"),
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(4, list.getTotalSize());

        // status filter
        val config = getTestConfig();
        val jobMgr = StreamingJobManager.getInstance(config, PROJECT);
        jobMgr.updateStreamingJob(MODEL_ID + "_build", copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Arrays.asList("RUNNING"),
                PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(3, list.getTotalSize());

        // project filter
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                PROJECT, "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 4);
        Assert.assertEquals(4, list.getTotalSize());

        // sort & reverse
        Assert.assertTrue(
                list.getValue().get(0).getLastUpdateTime().compareTo(list.getValue().get(1).getLastUpdateTime()) > 0);
        Assert.assertTrue(
                list.getValue().get(1).getLastUpdateTime().compareTo(list.getValue().get(2).getLastUpdateTime()) > 0);
        Assert.assertTrue(
                list.getValue().get(2).getLastUpdateTime().compareTo(list.getValue().get(3).getLastUpdateTime()) > 0);

        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                "", "last_update_time", false);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertTrue(
                list.getValue().get(0).getLastUpdateTime().compareTo(list.getValue().get(1).getLastUpdateTime()) < 0);
        Assert.assertTrue(
                list.getValue().get(1).getLastUpdateTime().compareTo(list.getValue().get(2).getLastUpdateTime()) < 0);
        Assert.assertTrue(
                list.getValue().get(2).getLastUpdateTime().compareTo(list.getValue().get(3).getLastUpdateTime()) < 0);

        // project & page_size filter
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                "", "last_update_time", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 4);
        Assert.assertEquals(4, list.getTotalSize());

        // offset filter
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 2);
        Assert.assertEquals(2, list.getTotalSize());
        streamingJobsStatsManager.deleteAllStreamingJobStats();
    }

    @Test
    public void testGetStreamingJobListOfIndex() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val streamingJobsStatsManager = createStatData(jobId);

        var jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_update_time", true);
        var list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(8, list.getTotalSize());
        Assert.assertEquals("model_streaming", list.getValue().get(0).getModelName());
        Assert.assertEquals(4, list.getValue().get(0).getModelIndexes().intValue());
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4, mgr.getIndexPlan("4965c827-fbb4-4ea1-a744-3f341a3b030d").getAllLayouts().size());
        Assert.assertEquals(4, mgr.getIndexPlan("cd2b9a23-699c-4699-b0dd-38c9412b3dfd").getAllLayouts().size());

        streamingJobsStatsManager.deleteAllStreamingJobStats();
    }

    @Test
    public void testGetStreamingJobDataStats() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val streamingJobsStatsManager = mockStreamingJobDataStats(jobId);
        val meta = streamingJobService.getStreamingJobDataStats(jobId, PROJECT, 1);
        Assert.assertEquals("500,400", StringUtils.join(meta.getDataLatencyHist(), ","));
        Assert.assertEquals("32,8", StringUtils.join(meta.getConsumptionRateHist(), ","));
        Assert.assertEquals("1200,3200", StringUtils.join(meta.getProcessingTimeHist(), ","));
        streamingJobsStatsManager.dropTable();
    }

    private StreamingJobStatsManager mockStreamingJobDataStats(String jobId) {
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        val streamingJobsStatsManager = StreamingJobStatsManager.getInstance();
        val now = System.currentTimeMillis();
        streamingJobsStatsManager
                .insert(new StreamingJobStats(jobId, PROJECT, 120L, 32.22, 1200L, 500L, 600L, now - 300000));
        streamingJobsStatsManager
                .insert(new StreamingJobStats(jobId, PROJECT, 120L, 8.17, 3200L, 400L, 800L, now - 400000));
        return streamingJobsStatsManager;
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
        streamingJobService.updateStreamingJobParams(PROJECT,
                StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name()), buildParam);

        mergeParam.put("spark.executor.memory", "3g");
        mergeParam.put("spark.master", "yarn");
        mergeParam.put("spark.driver.memory", "3g");
        mergeParam.put("kylin.streaming.segment-merge-threshold", "5");
        mergeParam.put("spark.executor.cores", "3");
        mergeParam.put("spark.executor.instances", "6");
        buildParam.put("kylin.streaming.job-retry-enabled", "true");
        mergeParam.put("spark.sql.shuffle.partitions", "20");
        streamingJobService.updateStreamingJobParams(PROJECT,
                StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name()), mergeParam);
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
                createKafkaPartitionsOffset(3, 500L), createKafkaPartitionsOffset(3, 600L));
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
        val req = new StreamingJobStatsRequest(jobId, PROJECT, 123L, 123.2, 42L, 30L, 50L, 60L);
        streamingJobService.collectStreamingJobStats(req);
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
        val streamingJobsStatsManager = StreamingJobStatsManager.getInstance();
        streamingJobsStatsManager.deleteAllStreamingJobStats();

        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val resp = streamingJobService.getStreamingJobInfo(jobId, PROJECT);
        Assert.assertEquals(JobStatusEnum.STOPPED, resp.getCurrentStatus());
        KylinConfig config = getTestConfig();

        val mgr = StreamingJobManager.getInstance(config, PROJECT);
        mgr.updateStreamingJob(jobId, copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                    Locale.getDefault(Locale.Category.FORMAT));
            copyForWrite.setLastUpdateTime(format.format(new Date()));
        });

        val resp1 = streamingJobService.getStreamingJobInfo(jobId, PROJECT);
        Assert.assertEquals(JobStatusEnum.RUNNING, resp1.getCurrentStatus());
        Assert.assertNotNull(resp1.getLastStatusDuration());
        Assert.assertNull(resp1.getDataLatency());
        Assert.assertNotNull(resp1.getLastUpdateTime());
    }

    @Test
    public void testGetStreamingJobInfo() {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        createStatData(jobId);
        val resp = streamingJobService.getStreamingJobInfo(jobId, PROJECT);
        Assert.assertEquals(JobStatusEnum.STOPPED, resp.getCurrentStatus());
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

        val resp1 = streamingJobService.getStreamingJobInfo(jobId, PROJECT);
        Assert.assertEquals(JobStatusEnum.RUNNING, resp1.getCurrentStatus());

        Assert.assertNull(resp1.getLastStatusDuration());
        Assert.assertNotNull(resp1.getDataLatency());
        Assert.assertNotNull(resp1.getLastUpdateTime());
        val streamingJobsStatsManager = StreamingJobStatsManager.getInstance();
        streamingJobsStatsManager.deleteAllStreamingJobStats();
    }

    @Test
    public void testGetStreamingJobRecordList() throws Exception {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        createRecordData(jobId);
        val list = streamingJobService.getStreamingJobRecordList(PROJECT, jobId);
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("START", list.get(0).getAction());
        Assert.assertEquals("STOP", list.get(1).getAction());
        Assert.assertEquals("START", list.get(2).getAction());
        Assert.assertTrue(list.get(0).getCreateTime() > list.get(1).getCreateTime());
        Assert.assertTrue(list.get(1).getCreateTime() > list.get(2).getCreateTime());
        val streamingJobsStatsManager = StreamingJobStatsManager.getInstance();
        streamingJobsStatsManager.deleteAllStreamingJobStats();
    }

    private StreamingJobStatsManager createStatData(String jobId) {
        val config = getTestConfig();
        config.setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");

        val streamingJobsStatsManager = StreamingJobStatsManager.getInstance();
        val now = System.currentTimeMillis();
        for (int i = 60; i > 0; i--) {
            val req = new StreamingJobStats(jobId, PROJECT, 120L, 32.22, 60000L, 500L, 60000L, now - i * 1000);
            streamingJobsStatsManager.insert(req);
        }
        return streamingJobsStatsManager;
    }

    private void createRecordData(String jobId) {
        val config = getTestConfig();
        config.setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        val record = new StreamingJobRecord();
        record.setId(100L);
        record.setJobId(jobId);
        record.setAction("START");
        record.setCreateTime(System.currentTimeMillis() - 90000);
        record.setUpdateTime(System.currentTimeMillis() - 90000);
        record.setProject(PROJECT);
        val mgr = StreamingJobRecordManager.getInstance();
        mgr.insert(record);

        val record1 = new StreamingJobRecord();
        BeanUtils.copyProperties(record, record1);
        record1.setId(101L);
        record1.setAction("STOP");
        record1.setCreateTime(System.currentTimeMillis() - 80000);
        mgr.insert(record1);

        val record2 = new StreamingJobRecord();
        BeanUtils.copyProperties(record, record2);
        record2.setId(102L);
        record2.setAction("START");
        record2.setCreateTime(System.currentTimeMillis() - 70000);
        mgr.insert(record2);
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
