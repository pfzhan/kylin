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

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import lombok.val;

public class AddSegHandlerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testWithLastSegment() {

        getTestConfig().setProperty("kylin.server.mode", "query");

        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        SegmentRange segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2013-01-01"),
                SegmentRange.dateToLong("2013-02-01"));
        NDataSegment dataSegment2 = dataflowManager.appendSegment(df, segmentRange2);

        dataSegment.setStatus(SegmentStatusEnum.READY);
        update.setToUpdateSegs(dataSegment);
        update.setToAddOrUpdateCuboids(genCuboids(df, dataSegment.getId()));
        dataflowManager.updateDataflow(update);

        AddSegmentEvent event = new AddSegmentEvent();
        event.setModelName("nmodel_basic");
        event.setCubePlanName("ncube_basic");
        event.setOwner("ADMIN");
        event.setJobId(UUID.randomUUID().toString());
        event.setSegmentId(dataSegment2.getId());

        EventContext eventContext = new EventContext(event, getTestConfig(), DEFAULT_PROJECT);
        AddSegmentHandler handler = new AddSegmentHandler();
        handler.handle(eventContext);

        String jobId = ((AddSegmentEvent) eventContext.getEvent()).getJobId();
        AbstractExecutable job = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId);
        Assert.assertNotNull(job);
        Assert.assertEquals(dataSegment2.getId(), ((ChainedExecutable) job).getTasks().get(1).getParam("segmentIds"));
        Assert.assertEquals(Joiner.on(",")
                .join(Stream.of(((ChainedExecutable) job).getTasks().get(1).getParam("cuboidLayoutIds").split(","))
                        .sorted(Comparator.comparing(a -> Long.parseLong(a))).collect(Collectors.toList())),
                Joiner.on(",").join(Stream.of(update.getToAddOrUpdateCuboids()).map(c -> c.getCuboidLayoutId())
                        .sorted(Comparator.naturalOrder()).collect(Collectors.toList())));
        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testAddSegment_WrongSegmentId() {

        AddSegmentEvent event2 = new AddSegmentEvent();
        event2.setModelName("nmodel_basic");
        event2.setCubePlanName("ncube_basic");
        event2.setOwner("ADMIN");
        event2.setJobId(UUID.randomUUID().toString());
        event2.setSegmentId(UUID.randomUUID().toString());
        EventContext eventContext2 = new EventContext(event2, getTestConfig(), DEFAULT_PROJECT);
        event2.getEventHandler().handle(eventContext2);
        String jobId2 = ((AddSegmentEvent) eventContext2.getEvent()).getJobId();
        AbstractExecutable job2 = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId2);
        Assert.assertNull(job2);

    }

    @Test
    public void testWithoutOtherSegment() throws Exception {

        getTestConfig().setProperty("kylin.server.mode", "query");

        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");

        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

        AddSegmentEvent event = new AddSegmentEvent();
        event.setModelName("nmodel_basic");
        event.setCubePlanName("ncube_basic");
        event.setOwner("ADMIN");
        event.setJobId(UUID.randomUUID().toString());
        event.setSegmentId(dataSegment.getId());

        EventContext eventContext = new EventContext(event, getTestConfig(), DEFAULT_PROJECT);
        AddSegmentHandler handler = new AddSegmentHandler();

        handler.handle(eventContext);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 0);

        String jobId = ((AddSegmentEvent) eventContext.getEvent()).getJobId();
        Assert.assertNotNull(jobId);

        AbstractExecutable job = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId);
        NDataflow df2 = dataflowManager.getDataflow("ncube_basic");
        Assert.assertNotNull(job);
        Assert.assertEquals(df2.getSegments().get(0).getId(),
                ((ChainedExecutable) job).getTasks().get(1).getParam("segmentIds"));
        Assert.assertEquals(Joiner.on(",")
                .join(Stream.of(((ChainedExecutable) job).getTasks().get(1).getParam("cuboidLayoutIds").split(","))
                        .sorted(Comparator.comparing(a -> Long.parseLong(a))).collect(Collectors.toList())),
                Joiner.on(",")
                        .join(NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getCubePlan("ncube_basic")
                                .getAllCuboidLayouts().stream().map(a -> a.getId()).sorted(Comparator.naturalOrder())
                                .collect(Collectors.toList())));

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    public NDataCuboid[] genCuboids(NDataflow df, String segId) {
        val dc1 = NDataCuboid.newDataCuboid(df, segId, 1L);
        val dc2 = NDataCuboid.newDataCuboid(df, segId, 1001L);
        return new NDataCuboid[] { dc1, dc2 };
    }

}
