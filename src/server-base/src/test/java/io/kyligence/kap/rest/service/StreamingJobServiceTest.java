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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.PartitionDesc;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.metadata.streaming.StreamingJobRecordManager;
import io.kyligence.kap.metadata.streaming.StreamingJobStats;
import io.kyligence.kap.metadata.streaming.StreamingJobStatsManager;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.request.StreamingJobActionEnum;
import io.kyligence.kap.rest.request.StreamingJobFilter;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.kyligence.kap.streaming.request.StreamingSegmentRequest;
import lombok.val;
import lombok.var;

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
        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);
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
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        var list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(10, list.getTotalSize());
        Assert.assertTrue(!list.getValue().get(0).isModelBroken());
        Assert.assertNotNull(list.getValue().get(0).getPartitionDesc());

        // modelName filter
        jobFilter = new StreamingJobFilter("stream_merge", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(6, list.getTotalSize());

        jobFilter = new StreamingJobFilter("stream_merge1", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(2, list.getTotalSize());

        jobFilter = new StreamingJobFilter("stream_merge2", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(0, list.getTotalSize());

        jobFilter = new StreamingJobFilter("", Arrays.asList("stream_merge1"), Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(2, list.getTotalSize());

        // job types filter
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Arrays.asList("STREAMING_BUILD"),
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(5, list.getValue().size());

        // status filter
        val config = getTestConfig();
        val jobMgr = StreamingJobManager.getInstance(config, PROJECT);
        jobMgr.updateStreamingJob(MODEL_ID + "_build", copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.RUNNING);
        });
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Arrays.asList("RUNNING"),
                PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(3, list.getTotalSize());

        // project filter
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 4);
        Assert.assertEquals(4, list.getValue().size());

        // sort & reverse
        Assert.assertTrue(list.getValue().get(0).getLastModified() >= list.getValue().get(1).getLastModified());
        Assert.assertTrue(list.getValue().get(1).getLastModified() >= list.getValue().get(2).getLastModified());
        Assert.assertTrue(list.getValue().get(2).getLastModified() >= list.getValue().get(3).getLastModified());

        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                "", "last_modified", false);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertTrue(list.getValue().get(0).getLastModified() >= list.getValue().get(1).getLastModified());
        Assert.assertTrue(list.getValue().get(1).getLastModified() >= list.getValue().get(2).getLastModified());
        Assert.assertTrue(list.getValue().get(2).getLastModified() >= list.getValue().get(3).getLastModified());

        // project & page_size filter
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                "", "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 4);
        Assert.assertEquals(10, list.getTotalSize());
        Assert.assertEquals(4, list.getValue().size());

        // offset filter
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 2);
        Assert.assertEquals(2, list.getValue().size());
        streamingJobsStatsManager.deleteAllStreamingJobStats();

        jobMgr.updateStreamingJob(MODEL_ID + "_build", copyForWrite -> {
            copyForWrite.setCurrentStatus(JobStatusEnum.LAUNCHING_ERROR);
        });
        jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST, Arrays.asList("ERROR"),
                PROJECT, "last_modified", true);
        list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(1, list.getTotalSize());
        Assert.assertTrue(list.getValue().get(0).isLaunchingError());

        StreamingJobMeta streamingJobMeta = jobMgr.getStreamingJobByUuid(MODEL_ID + "_build");
        Assert.assertEquals(JobStatusEnum.LAUNCHING_ERROR, streamingJobMeta.getCurrentStatus());
    }

    @Test
    public void testGetStreamingJobListOfIndex() {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val streamingJobsStatsManager = createStatData(jobId);

        var jobFilter = new StreamingJobFilter("", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, PROJECT, "last_modified", true);
        var list = streamingJobService.getStreamingJobList(jobFilter, 0, 20);
        Assert.assertEquals(10, list.getTotalSize());
        Assert.assertEquals("model_streaming", list.getValue().get(2).getModelName());
        Assert.assertEquals(4, list.getValue().get(2).getModelIndexes().intValue());
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4, mgr.getIndexPlan("4965c827-fbb4-4ea1-a744-3f341a3b030d").getAllLayouts().size());
        Assert.assertEquals(4, mgr.getIndexPlan("cd2b9a23-699c-4699-b0dd-38c9412b3dfd").getAllLayouts().size());

        streamingJobsStatsManager.deleteAllStreamingJobStats();
    }

    @Test
    public void testIsBatchModelBroken() {
        val model = Mockito.spy(NDataModel.class);
        Mockito.when(model.isFusionModel()).thenReturn(false);
        val result = streamingJobService.isBatchModelBroken(model);
        Assert.assertFalse(result);
    }

    @Test
    public void testIsBatchModelBroken1() {
        val model = Mockito.spy(NDataModel.class);
        Mockito.when(model.isFusionModel()).thenReturn(true);
        model.setFusionId("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        val result = streamingJobService.isBatchModelBroken(model);
        Assert.assertTrue(result);
        val result1 = streamingJobService.isBatchModelBroken(null);
        Assert.assertTrue(result1);
    }

    @Test
    public void testGetStreamingJobDataStats() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val streamingJobsStatsManager = mockStreamingJobDataStats(jobId);
        val meta1 = streamingJobService.getStreamingJobDataStats(jobId, 1);
        Assert.assertEquals("500,400", StringUtils.join(meta1.getDataLatencyHist(), ","));
        Assert.assertEquals("32,8", StringUtils.join(meta1.getConsumptionRateHist(), ","));
        Assert.assertEquals("1200,3200", StringUtils.join(meta1.getProcessingTimeHist(), ","));
        val meta3 = streamingJobService.getStreamingJobDataStats(jobId, 3);
        Assert.assertNotNull(meta3);
        val meta7 = streamingJobService.getStreamingJobDataStats(jobId, 7);
        Assert.assertNotNull(meta7);
        val meta8 = streamingJobService.getStreamingJobDataStats(jobId, 0);
        Assert.assertNull(meta8.getConsumptionRateHist());
        streamingJobsStatsManager.dropTable();
    }

    @Test
    public void testGetStreamingJobDataStatsException() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val streamingJobsStatsManager = mockStreamingJobDataStats(jobId);
        try {
            streamingJobService.getStreamingJobDataStats(jobId, 9);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        } finally {
            streamingJobsStatsManager.dropTable();
        }
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
        streamingJobService.updateStreamingJobStatus(PROJECT, createJobList(MODEL_ID), "START");
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
    public void testUpdateStatusOfNullPrj() throws Exception {
        streamingJobService.updateStreamingJobStatus(null, createJobList(MODEL_ID), "START");
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
    public void testUpdateStatusOfEmptyProject() throws Exception {
        streamingJobService.updateStreamingJobStatus(StringUtils.EMPTY, createJobList(MODEL_ID), "START");
        streamingJobService.updateStreamingJobStatus(StringUtils.EMPTY, createJobList(MODEL_ID), "STOP");
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
    public void testUpdateStreamingJobStatusToStop() throws Exception {
        streamingJobService.updateStreamingJobStatus(PROJECT, createJobList(MODEL_ID), "START");
        streamingJobService.updateStreamingJobStatus(PROJECT, createJobList(MODEL_ID), "STOP");
        KylinConfig testConfig = getTestConfig();

        StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(testConfig, PROJECT);
        String buildJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val buildMeta = streamingJobManager.getStreamingJobByUuid(buildJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildMeta.getCurrentStatus());

        String mergeJobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name());
        val mergeMeta = streamingJobManager.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeMeta.getCurrentStatus());
    }

    private List<String> createJobList(String modelId) {
        return Arrays.asList(StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name()),
                StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name()));
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
        val newSegId = RandomUtil.randomUUIDStr();
        streamingJobService.addSegment(PROJECT, DATAFLOW_ID, rangeToMerge, "0", newSegId);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val newSeg = df.getSegment(newSegId);
        Assert.assertEquals(newSegId, newSeg.getId());
        Assert.assertEquals("1", newSeg.getAdditionalInfo().get(StreamingConstants.FILE_LAYER));
        val segId = streamingJobService.addSegment(PROJECT, "not_existed_model", rangeToMerge, "0", newSegId);
        Assert.assertEquals(StringUtils.EMPTY, segId);
    }

    @Test
    public void testAppendSegment() throws Exception {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957140000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 500L), createKafkaPartitionsOffset(3, 600L));
        val newSegId = RandomUtil.randomUUIDStr();
        streamingJobService.addSegment(PROJECT, DATAFLOW_ID, rangeToMerge, null, newSegId);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        val newSeg = df.getSegment(newSegId);
        Assert.assertEquals(newSegId, newSeg.getId());
        Assert.assertNull(newSeg.getAdditionalInfo().get(StreamingConstants.FILE_LAYER));
        val segId =streamingJobService.addSegment(PROJECT, "not_existed_model", rangeToMerge, null, newSegId);
        Assert.assertEquals(StringUtils.EMPTY, segId);

    }

    @Test
    public void testUpdateSegmentForOnline() throws Exception {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236901";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        streamingJobService.updateSegment(PROJECT, dataflowId, segId, null, "ONLINE", 0L);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);

        Assert.assertEquals(SegmentStatusEnum.READY, seg.getStatus());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
    }

    @Test
    public void testUpdateSegmentForCount() throws Exception {
        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236901";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        streamingJobService.updateSegment(PROJECT, dataflowId, segId, null, "ONLINE", 100L);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);
        Assert.assertEquals(100L, seg.getSourceCount());

        StreamingSegmentRequest request = new StreamingSegmentRequest(PROJECT, dataflowId, 200L);
        request.setStatus("ONLINE");
        streamingJobService.updateSegment(request.getProject(), request.getDataflowId(), segId, null, request.getStatus(), request.getSourceCount());
        NDataflow df2 = mgr.getDataflow(request.getDataflowId());
        val seg2 = df2.getSegment(segId);
        Assert.assertEquals(200L, seg2.getSourceCount());
        Assert.assertEquals(Long.valueOf(200), request.getSourceCount());

        StreamingSegmentRequest request2 = new StreamingSegmentRequest(PROJECT, dataflowId);
        request2.setStatus("ONLINE");
        streamingJobService.updateSegment(request2.getProject(), request2.getDataflowId(), segId, null, request2.getStatus(), request2.getSourceCount());
        NDataflow df3 = mgr.getDataflow(request2.getDataflowId());
        val seg3 = df3.getSegment(segId);
        Assert.assertEquals(200L, seg3.getSourceCount());
        Assert.assertEquals(Long.valueOf(-1), request2.getSourceCount());
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
    public void testCollectStreamingJobStatsException() {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        Mockito.when(streamingJobService.getStreamingJobStatsManager()).thenReturn(null);
        val req = new StreamingJobStatsRequest(jobId, PROJECT, 123L, 123.2, 42L, 30L, 50L, 60L);
        try{
            streamingJobService.collectStreamingJobStats(req);
        }catch (Exception e) {
            Assert.fail();
        }
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
        val list = streamingJobService.getStreamingJobRecordList(jobId);
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
        val jobIds = Arrays.asList(StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name()),
                StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name()));
        streamingJobService.updateStreamingJobStatus(PROJECT, jobIds, StreamingJobActionEnum.FORCE_STOP.name());
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.STOPPED, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.STOPPED, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testRestartStreamingJob() {
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
        val jobIds = Arrays.asList(StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name()),
                StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_MERGE.name()));
        streamingJobService.updateStreamingJobStatus(PROJECT, jobIds, StreamingJobActionEnum.RESTART.name());
        val buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertEquals(JobStatusEnum.RUNNING, buildJobMeta.getCurrentStatus());
        Assert.assertEquals(JobStatusEnum.RUNNING, mergeJobMeta.getCurrentStatus());
    }

    @Test
    public void testGetStreamingJobSimpleLog() throws IOException {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de_build";
        String project = "default";

        String[] exceptLines = createStreamingLogTmpFile(project, jobId);

        String verboseMsg = streamingJobService.getStreamingJobSimpleLog(project, jobId);
        String[] actualVerboseMsgLines = org.apache.commons.lang.StringUtils
                .splitByWholeSeparatorPreserveAllTokens(verboseMsg, "\n");
        ArrayList<String> exceptLinesL = Lists.newArrayList(exceptLines);
        exceptLinesL.add("================================================================");
        Assert.assertTrue(Sets.newHashSet(exceptLinesL).containsAll(Sets.newHashSet(actualVerboseMsgLines)));
    }

    @Test
    public void testGetStreamingJobAllLog() throws IOException {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de_build";
        String project = "default";

        String[] exceptLines = createStreamingLogTmpFile(project, jobId);

        String sampleLog = "";
        try (InputStream inputStream = streamingJobService.getStreamingJobAllLog(project, jobId);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(inputStream, Charset.defaultCharset()))) {
            String line;
            StringBuilder sampleData = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                if (sampleData.length() > 0) {
                    sampleData.append('\n');
                }
                sampleData.append(line);
            }

            sampleLog = sampleData.toString();
        }
        String[] actualLines = org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens(sampleLog,
                "\n");
        Assert.assertTrue(Arrays.deepEquals(exceptLines, actualLines));
    }

    public String[] createStreamingLogTmpFile(String project, String jobId) throws IOException {

        File file = temporaryFolder.newFile("driver." + System.currentTimeMillis() + ".log");
        for (int i = 0; i < 200; i++) {
            Files.write(file.toPath(), String.format(Locale.ROOT, "lines: %s\n", i).getBytes(Charset.defaultCharset()),
                    StandardOpenOption.APPEND);
        }

        String[] exceptLines = Files.readAllLines(file.toPath()).toArray(new String[0]);
        String jobLogDir = KylinConfig.getInstanceFromEnv().getStreamingJobTmpOutputStorePath(project, jobId);

        Path jobLogDirPath = new Path(jobLogDir);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(jobLogDirPath);
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), jobLogDirPath);

        return exceptLines;
    }

    @Test
    public void testCheckModelStatus() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val config = getTestConfig();

        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);

        val buildJobId = modelId + "_build";
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);

        try {
            streamingJobService.checkModelStatus(PROJECT, modelId, buildJobMeta.getJobType());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    /**
     * streaming model is broken
     */
    @Test
    public void testCheckModelStatus1() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);

        val buildJobId = modelId + "_build";
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        val kafkaConfMgr = KafkaConfigManager.getInstance(config, PROJECT);
        kafkaConfMgr.removeKafkaConfig("SSB.P_LINEORDER_STR");
        val model = NDataModelManager.getInstance(config, PROJECT).getDataModelDesc(modelId);
        Assert.assertTrue(model.isBroken());
        thrown.expect(KylinException.class);
        streamingJobService.checkModelStatus(PROJECT, modelId, buildJobMeta.getJobType());
    }

    /**
     * batch model of fusion model is broken
     */
    @Test
    public void testCheckModelStatus2() {
        String modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        val config = getTestConfig();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, PROJECT);

        val buildJobId = modelId + "_build";
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);

        val tblMetaMgr = NTableMetadataManager.getInstance(config, PROJECT);
        tblMetaMgr.removeSourceTable("SSB.LINEORDER_HIVE");

        val modelMgr = NDataModelManager.getInstance(config, PROJECT);
        val streamingModel = modelMgr.getDataModelDesc(modelId);
        Assert.assertFalse(streamingModel.isBroken());

        val batchModelId = "cd2b9a23-699c-4699-b0dd-38c9412b3dfd";
        val batchModel = modelMgr.getDataModelDesc(batchModelId);
        Assert.assertTrue(batchModel.isBroken());

        thrown.expect(KylinException.class);
        streamingJobService.checkModelStatus(PROJECT, modelId, buildJobMeta.getJobType());
    }

    @Test
    public void testCheckPartitionColumn() {
        val config = getTestConfig();
        val mgr = NDataModelManager.getInstance(config, PROJECT);
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        mgr.updateDataModel(modelId, copy -> {
            copy.setPartitionDesc(null);
        });
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getPARTITION_COLUMN_START_ERROR());
        streamingJobService.launchStreamingJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
    }

    @Test
    public void testCheckPartitionColumn1() {
        val config = getTestConfig();
        val mgr = NDataModelManager.getInstance(config, PROJECT);
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        mgr.updateDataModel(modelId, copy -> {
            copy.setPartitionDesc(Mockito.spy(PartitionDesc.class));
        });
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getPARTITION_COLUMN_START_ERROR());
        streamingJobService.launchStreamingJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
    }

    @Test
    public void testCheckPartitionColumn2() {
        val config = getTestConfig();
        val mgr = NDataModelManager.getInstance(config, PROJECT);
        val modelId = "4965c827-fbb4-4ea1-a744-3f341a3b030d";
        mgr.updateDataModel(modelId, copy -> {
            val mock = Mockito.spy(PartitionDesc.class);
            mock.setPartitionDateFormat("yyyy/MM/dd");
            copy.setPartitionDesc(mock);
        });
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getPARTITION_COLUMN_START_ERROR());
        streamingJobService.launchStreamingJob(PROJECT, modelId, JobTypeEnum.STREAMING_BUILD);
    }
}
