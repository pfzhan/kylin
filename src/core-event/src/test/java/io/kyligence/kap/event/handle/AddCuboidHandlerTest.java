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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventDao;
import org.mockito.Mock;
import org.mockito.Mockito;

public class AddCuboidHandlerTest extends NLocalFileMetadataTestCase {

    @Mock
    private FavoriteQueryJDBCDao favoriteQueryJDBCDao = Mockito.mock(FavoriteQueryJDBCDao.class);

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHandlerIdempotent() throws Exception {


        // first add cuboid layouts
        String sqlPattern = "select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT";
        List<String> sqls = Lists.<String> newArrayList(sqlPattern);
        NSmartMaster master = new NSmartMaster(getTestConfig(), DEFAULT_PROJECT, sqls.toArray(new String[0]));
        master.runAll();

        List<NSmartContext.NModelContext> contexts = master.getContext().getModelContexts();
        List<Long> addedCuboidLayoutIds = calcAddedCuboidLayoutIds(contexts);

        AddCuboidEvent event = new AddCuboidEvent();
        event.setApproved(true);
        event.setProject(DEFAULT_PROJECT);
        event.setModelName("nmodel_basic");
        event.setCubePlanName("ncube_basic");
        event.setLayoutIds(Lists.<Long> newArrayList(addedCuboidLayoutIds));
        event.setSqlPatterns(Lists.<String> newArrayList());
        EventContext eventContext = new EventContext(event, getTestConfig());
        val handler = Mockito.spy(new AddCuboidHandler());
        Mockito.doReturn(favoriteQueryJDBCDao).when(handler).getFavoriteQueryDao();
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


    }

    @Test
    public void testHandler_FullLoad_Pass() throws Exception {
        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        getTestConfig().setProperty("kylin.server.mode", "query");

        // first add cuboid layouts
        String sqlPattern = "select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT";
        List<String> sqls = Lists.<String> newArrayList(sqlPattern);
        NSmartMaster master = new NSmartMaster(getTestConfig(), DEFAULT_PROJECT, sqls.toArray(new String[0]));
        master.runAll();

        List<NSmartContext.NModelContext> contexts = master.getContext().getModelContexts();
        List<Long> addedCuboidLayoutIds = calcAddedCuboidLayoutIds(contexts);

        AddCuboidEvent event = new AddCuboidEvent();
        event.setApproved(true);
        event.setProject(DEFAULT_PROJECT);
        event.setModelName("nmodel_basic");
        event.setCubePlanName("ncube_basic");
        event.setLayoutIds(Lists.<Long> newArrayList(addedCuboidLayoutIds));
        event.setSqlPatterns(Lists.<String> newArrayList());
        EventContext eventContext = new EventContext(event, getTestConfig());
        val handler = Mockito.spy(new AddCuboidHandler());
        Mockito.doReturn(favoriteQueryJDBCDao).when(handler).getFavoriteQueryDao();
        eventDao.deleteAllEvents();
        handler.handle(eventContext);

        List<Event> events = eventDao.getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 1);

        String jobId = eventContext.getEvent().getJobId();
        Assert.assertNotNull(jobId);

        AbstractExecutable job = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId);
        Assert.assertNotNull(job);
        Assert.assertEquals(0L, job.getDataRangeStart());
        Assert.assertEquals(Long.MAX_VALUE, job.getDataRangeEnd());

    }

    private List<Long> calcAddedCuboidLayoutIds(List<NSmartContext.NModelContext> contexts) {
        List<Long> originLayoutIds = new ArrayList<>();
        List<Long> targetLayoutIds = new ArrayList<>();

        NSmartContext.NModelContext context = contexts.get(0);
        NCubePlan originCubePlan = context.getOrigCubePlan();
        NCubePlan targetCubePlan = context.getTargetCubePlan();
        for (NCuboidLayout layout : originCubePlan.getAllCuboidLayouts()) {
            originLayoutIds.add(layout.getId());
        }
        for (NCuboidLayout layout : targetCubePlan.getAllCuboidLayouts()) {
            targetLayoutIds.add(layout.getId());
        }

        targetLayoutIds.removeAll(originLayoutIds);

        return targetLayoutIds;
    }

}
