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

import java.util.UUID;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;
import lombok.var;

public class PostAddCuboidHandlerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRestartNoJobForSuicideJob_SuicideByCuttingJobAndSegmentsMissing() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val sql = "select * from test_kylin_fact";
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-09-01"),
                SegmentRange.dateToLong("2012-10-01"));
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        val handler = ((DefaultChainedExecutableOnModel) job).getHandler();
        handler.handleDiscardOrSuicidal();
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(0, events.size());
    }

    private AbstractExecutable mockJob(String jobId, long start, long end) {
        return mockJob(jobId, start, end, "default");
    }

    private AbstractExecutable mockJob(String jobId, long start, long end, String project) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        var dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        val oneSeg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(start, end));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_BUILD, jobId);
        NExecutableManager.getInstance(getTestConfig(), project).addJob(job);
        return NExecutableManager.getInstance(getTestConfig(), project).getJob(jobId);
    }

    @Test
    public void testRestartNewJobForSuicideJob_SuicideByCuttingJob() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));

        val handler = ((DefaultChainedExecutableOnModel) job).getHandler();
        mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-09-01"),
                SegmentRange.dateToLong("2012-10-01"));

        handler.handleDiscardOrSuicidal();
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(1, events.size());
        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);
    }

    @Test
    public void testRestartNewJobForSuicideJob_SuicideBySegmentsMissing() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val handler = ((DefaultChainedExecutableOnModel) job).getHandler();
        handler.handleDiscardOrSuicidal();
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(0, events.size());
    }

    private void cleanModel(String dataflowId) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow(dataflowId);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getId());
        update.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

}
