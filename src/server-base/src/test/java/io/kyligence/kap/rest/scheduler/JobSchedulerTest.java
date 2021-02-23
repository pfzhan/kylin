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

package io.kyligence.kap.rest.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.job.ExecutableAddCuboidHandler;
import io.kyligence.kap.engine.spark.job.ExecutableAddSegmentHandler;
import io.kyligence.kap.engine.spark.job.ExecutableMergeOrRefreshHandler;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobSchedulerTest extends NLocalFileMetadataTestCase {

    public static final String DEFAULT_PROJECT = "default";
    public static final String MODEL_ID = "741ca86a-1f13-46da-a59f-95fb68615e3a";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        ExecutableUtils.initJobFactory();
        createTestMetadata();
        prepareSegment();
    }

    @Test
    public void testAddIndex_chooseIndexAndSegment() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val oldSegs = new NDataSegment(df.getSegments().get(0));
        val update = new NDataflowUpdate(df.getUuid());
        val oldLayouts = new ArrayList<>(df.getSegments().get(0).getLayoutsMap().values());
        update.setToUpdateSegs(oldSegs);
        update.setToRemoveLayouts(oldLayouts.get(0), oldLayouts.get(1));
        dfm.updateDataflow(update);

        // select some segments and indexes
        HashSet<String> relatedSegments = Sets.newHashSet();
        relatedSegments.add(oldSegs.getId());
        val targetLayouts = new HashSet<Long>();
        targetLayouts.add(1L);
        targetLayouts.add(10001L);
        val jobId = jobManager.addRelatedIndexJob(new JobParam(relatedSegments, targetLayouts, MODEL_ID, "ADMIN"));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertNotNull(jobId);
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
        Assert.assertEquals(1, getProcessLayout(executables.get(0)));

        // auto select valid segment
        val jobId2 = jobManager.addIndexJob(new JobParam(MODEL_ID, "ADMIN"));
        Assert.assertNull(jobId2);
    }

    @Test
    public void testAddIndex_selectNoIndex() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val update = new NDataflowUpdate(df.getUuid());
        val seg = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-05-01"), SegmentRange.dateToLong("" + "2012-06-01")));
        seg.setStatus(SegmentStatusEnum.READY);
        update.setToUpdateSegs(seg);
        update.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df, seg.getId(), 1L),
                NDataLayout.newDataLayout(df, seg.getId(), 10001L), NDataLayout.newDataLayout(df, seg.getId(), 10002L));
        dfm.updateDataflow(update);

        jobManager.addIndexJob(new JobParam(MODEL_ID, "ADMIN"));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
        Assert.assertEquals(16, getProcessLayout(executables.get(0)));
    }

    @Test
    public void testAddIndex_timeException() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val oldSegs = new NDataSegment(df.getSegments().get(0));
        val update = new NDataflowUpdate(df.getUuid());
        val oldLayouts = new ArrayList<>(df.getSegments().get(0).getLayoutsMap().values());
        update.setToUpdateSegs(oldSegs);
        update.setToRemoveLayouts(oldLayouts.get(0), oldLayouts.get(1));
        dfm.updateDataflow(update);

        HashSet<String> relatedSegments = Sets.newHashSet();
        df.getSegments().forEach(seg -> relatedSegments.add(seg.getId()));
        val targetLayouts = new HashSet<Long>();
        targetLayouts.add(1L);
        targetLayouts.add(10001L);
        jobManager.addRelatedIndexJob(new JobParam(relatedSegments, targetLayouts, MODEL_ID, "ADMIN"));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddCuboidHandler);
        Assert.assertEquals(1, getProcessLayout(executables.get(0)));

        relatedSegments.remove(0);
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        jobManager.addRelatedIndexJob(new JobParam(relatedSegments, targetLayouts, MODEL_ID, "ADMIN"));
    }

    @Test
    public void testRefreshJob() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        jobManager.refreshSegmentJob(new JobParam(seg, MODEL_ID, "ADMIN"));

        val seg2 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        jobManager.refreshSegmentJob(new JobParam(seg2, MODEL_ID, "ADMIN"));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertTrue(
                ((NSparkCubingJob) executables.get(1)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertEquals(19, getProcessLayout(executables.get(0)));
        Assert.assertEquals(19, getProcessLayout(executables.get(1)));

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        jobManager.refreshSegmentJob(new JobParam(seg, MODEL_ID, "ADMIN"));
    }

    @Test
    public void testRefreshJob_timeException() {

        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        jobManager.refreshSegmentJob(new JobParam(seg, MODEL_ID, "ADMIN"));
        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(1, executables.size());

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        jobManager.refreshSegmentJob(new JobParam(seg, MODEL_ID, "ADMIN"));
    }

    @Test
    public void testRefreshJob_emptyIndex() {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dfManager.getDataflow(MODEL_ID);
        val update = new NDataflowUpdate(df.getUuid());
        val seg = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-05-01"), SegmentRange.dateToLong("" + "2012-06-01")));
        seg.setStatus(SegmentStatusEnum.READY);
        update.setToUpdateSegs(seg);
        dfManager.updateDataflow(update);
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        jobManager.refreshSegmentJob(new JobParam(seg, MODEL_ID, "ADMIN"), true);
        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(1, executables.size());
        Assert.assertEquals(19, executables.get(0).getParam(NBatchConstants.P_LAYOUT_IDS).split(",").length);
    }

    @Test
    public void testMergeJob() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        val seg2 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-03-01"), SegmentRange.dateToLong("" + "2012-05-01")));
        jobManager.mergeSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
        jobManager.mergeSegmentJob(new JobParam(seg2, MODEL_ID, "ADMIN"));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(
                ((NSparkMergingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertEquals(19, getProcessLayout(executables.get(0)));
        Assert.assertTrue(
                ((NSparkMergingJob) executables.get(0)).getHandler() instanceof ExecutableMergeOrRefreshHandler);
        Assert.assertEquals(19, getProcessLayout(executables.get(0)));
    }

    @Test
    public void testMergeJob_indexNotAlightedEception() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val oldSegs = new NDataSegment(df.getSegments().get(0));
        val update = new NDataflowUpdate(df.getUuid());
        val oldLayouts = new ArrayList<>(df.getSegments().get(0).getLayoutsMap().values());
        update.setToUpdateSegs(oldSegs);
        update.setToRemoveLayouts(oldLayouts.get(0), oldLayouts.get(1));
        dfm.updateDataflow(update);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t submit the job, as the indexes are not identical in the selected segments. Please check and try again.");
        jobManager.mergeSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
    }

    @Test
    public void testMergeJob_notReadySegmentException() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-09-01"), SegmentRange.dateToLong("" + "2012-10-01")));
        try {
            jobManager.mergeSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals("Can’t find executable jobs at the moment. Please try again later.", e.getMessage());
        }
    }

    @Test
    public void testMergeJob_timeEception() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        jobManager.mergeSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(1, executables.size());
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        jobManager.mergeSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
    }

    @Test
    public void testAddSegmentJob_selectNoSegments() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-05-01"), SegmentRange.dateToLong("" + "2012-06-01")));
        val seg2 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("" + "2012-07-01")));
        jobManager.addSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
        jobManager.addSegmentJob(new JobParam(seg2, MODEL_ID, "ADMIN"));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertEquals(19, getProcessLayout(executables.get(0)));
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertEquals(19, getProcessLayout(executables.get(0)));
    }

    @Test
    public void testAddSegmentJob_selectSegments() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-05-01"), SegmentRange.dateToLong("" + "2012-06-01")));
        val seg2 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("" + "2012-07-01")));
        HashSet<Long> targetLayouts = new HashSet<>();
        targetLayouts.add(1L);
        targetLayouts.add(10001L);
        jobManager.addSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN", targetLayouts));
        jobManager.addSegmentJob(new JobParam(seg2, MODEL_ID, "ADMIN", targetLayouts));

        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(2, executables.size());
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertEquals(2, getProcessLayout(executables.get(0)));
        Assert.assertTrue(((NSparkCubingJob) executables.get(0)).getHandler() instanceof ExecutableAddSegmentHandler);
        Assert.assertEquals(2, getProcessLayout(executables.get(0)));
    }

    @Test
    public void testAddSegmentJob_timeException() {
        val jobManager = JobManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dfm = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfm.getDataflow(MODEL_ID);

        val seg1 = dfm.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-05-01"), SegmentRange.dateToLong("" + "2012-06-01")));
        jobManager.addSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
        List<AbstractExecutable> executables = getRunningExecutables(DEFAULT_PROJECT, MODEL_ID);
        Assert.assertEquals(1, executables.size());

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        jobManager.addSegmentJob(new JobParam(seg1, MODEL_ID, "ADMIN"));
    }

    public void prepareSegment() {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dfManager.getDataflow(MODEL_ID);
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        val seg1 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val seg2 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));
        val seg3 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-03-01"), SegmentRange.dateToLong("" + "2012-04-01")));
        val seg4 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-04-01"), SegmentRange.dateToLong("" + "2012-05-01")));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg2.setStatus(SegmentStatusEnum.READY);
        seg3.setStatus(SegmentStatusEnum.READY);
        seg4.setStatus(SegmentStatusEnum.READY);
        val update2 = new NDataflowUpdate(df.getUuid());
        update2.setToUpdateSegs(seg1, seg2, seg3, seg4);
        List<NDataLayout> layouts = Lists.newArrayList();
        indexManager.getIndexPlan(MODEL_ID).getAllLayouts().forEach(layout -> {
            layouts.add(NDataLayout.newDataLayout(df, seg1.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(df, seg2.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(df, seg3.getId(), layout.getId()));
            layouts.add(NDataLayout.newDataLayout(df, seg4.getId(), layout.getId()));
        });
        update2.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
        dfManager.updateDataflow(update2);
    }

    private List<AbstractExecutable> getRunningExecutables(String project, String model) {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getRunningExecutables(project,
                model);
    }

    private int getProcessLayout(AbstractExecutable executable) {
        String layouts = executable.getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isBlank(layouts)) {
            return 0;
        }
        return layouts.split(",").length;
    }

}
