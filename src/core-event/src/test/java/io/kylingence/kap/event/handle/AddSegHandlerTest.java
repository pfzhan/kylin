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
package io.kylingence.kap.event.handle;

import java.util.List;

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kylingence.kap.event.model.AddSegmentEvent;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kylingence.kap.event.manager.EventDao;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.EventContext;

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
    public void testHandlerIdempotent() throws Exception {

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
        event.setApproved(true);
        event.setProject(DEFAULT_PROJECT);
        event.setModelName("nmodel_basic");
        event.setCubePlanName("ncube_basic");
        event.setSegmentIds(Lists.newArrayList(dataSegment.getId()));

        EventContext eventContext = new EventContext(event, getTestConfig());
        AddSegmentHandler handler = new AddSegmentHandler();

        handler.handle(eventContext);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 1);

        String jobId = eventContext.getEvent().getJobId();
        Assert.assertNotNull(jobId);

        AbstractExecutable job = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId);
        Assert.assertNotNull(job);

        // do handle again
        handler.handle(eventContext);

        String jobId2 = eventContext.getEvent().getJobId();
        Assert.assertNotNull(jobId);
        Assert.assertEquals(jobId, jobId2);

        int size = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getAllExecutables().size();
        Assert.assertEquals(size, 1);

        getTestConfig().setProperty("kylin.server.mode", "all");

    }

}
