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

package io.kyligence.kap.event.handle;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;
import lombok.var;

public class PostAddSegmentHandlerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testMarkModelOnline_WhenStoppedIncJobExist()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        var dataflow = dataflowManager.getDataflow(dataflowId);
        val executableManager = NExecutableManager.getInstance(getTestConfig(), project);

        val job = NSparkCubingJob.create(Sets.newHashSet(dataflow.getSegments()),
                Sets.newLinkedHashSet(dataflow.getIndexPlan().getAllLayouts()), "", JobTypeEnum.INC_BUILD,
                UUID.randomUUID().toString());
        executableManager.addJob(job);
        // pause IncJob will mark dataflow status to LAG_BEHIND
        executableManager.pauseJob(job.getId());

        testMarkDFOnlineIfNecessary();
        dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testMarkModelOnline_WhenDataLoadingRangeCannotCover()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.updateDataflow(dataflowId, copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
        });
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToRemoveSegs(dataflowManager.getDataflow(dataflowId).getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        dataflowManager.appendSegment(dataflowManager.getDataflow(dataflowId),
                SegmentRange.TimePartitionedSegmentRange.createInfinite());

        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), project);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        dataLoadingRange.setCoveredRange(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        testMarkDFOnlineIfNecessary();
        val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testMarkModelOnline_WhenHasNewSegInQueryableRange()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.updateDataflow(dataflowId, copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
        });
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToRemoveSegs(dataflowManager.getDataflow(dataflowId).getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        //prepare 3 segments (0,10)Ready (10,100)NEW (100, Long.max)Ready
        val seg1 = dataflowManager.appendSegment(dataflowManager.getDataflow(dataflowId),
                new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        dataflowManager.appendSegment(dataflowManager.getDataflow(dataflowId),
                new SegmentRange.TimePartitionedSegmentRange(10L, 100L));
        val seg2 = dataflowManager.appendSegment(dataflowManager.getDataflow(dataflowId),
                new SegmentRange.TimePartitionedSegmentRange(100L, Long.MAX_VALUE));

        update = new NDataflowUpdate(dataflowId);
        seg1.setStatus(SegmentStatusEnum.READY);
        seg2.setStatus(SegmentStatusEnum.READY);
        update.setToUpdateSegs(new NDataSegment[] { seg1, seg2 });
        dataflowManager.updateDataflow(update);

        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), project);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        dataLoadingRange.setCoveredRange(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        testMarkDFOnlineIfNecessary();
        val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testMarkModelOnline_WhenNoErrorJObAndDataLoadingRangeCover()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.updateDataflow(dataflowId, copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
        });

        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), project);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        dataLoadingRange.setCoveredRange(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        testMarkDFOnlineIfNecessary();
        val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dataflow.getStatus());
    }

    @Test
    public void testSegmentReady_WhenJobSuicideBecauseOfLayouts() {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        var dataflow = dataflowManager.getDataflow(dataflowId);
        val copy = dataflow.copy();
        val update = new NDataflowUpdate(dataflow.getId());
        val segment = copy.getFirstSegment();
        segment.setStatus(SegmentStatusEnum.NEW);
        update.setToUpdateSegs(segment);
        dataflowManager.updateDataflow(update);
        val executableManager = NExecutableManager.getInstance(getTestConfig(), project);

        val job = NSparkCubingJob.create(Sets.newHashSet(dataflow.getFirstSegment()),
                Sets.newLinkedHashSet(dataflow.getIndexPlan().getAllLayouts()), "", JobTypeEnum.INC_BUILD,
                UUID.randomUUID().toString());
        executableManager.addJob(job);
        // pause IncJob will mark dataflow status to LAG_BEHIND
        executableManager.updateJobOutput(job.getId(), ExecutableState.SUICIDAL);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> {
            copyForWrite.setIndexes(Lists.newArrayList());
        });

        val event = new PostAddSegmentEvent();
        event.setModelId(dataflowId);
        event.setSegmentId(dataflow.getFirstSegment().getId());
        event.setJobId(job.getId());

        event.getEventHandler().handle(new EventContext(event, getTestConfig(), project));

        dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(dataflow.getFirstSegment().getStatus(), SegmentStatusEnum.READY);
    }

    private void testMarkDFOnlineIfNecessary()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val postAddSegmentHandler = new PostAddSegmentHandler();
        val method = postAddSegmentHandler.getClass().getDeclaredMethod("markDFOnlineIfNecessary", NDataflow.class);
        method.setAccessible(true);
        var dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        method.invoke(postAddSegmentHandler, dataflow);
    }

}
