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

import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;

public class AddCuboidHandlerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
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

        AddCuboidEvent event = new AddCuboidEvent();
        event.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event.setOwner("ADMIN");
        EventContext eventContext = new EventContext(event, getTestConfig(), DEFAULT_PROJECT);
        val handler = Mockito.spy(new AddCuboidHandler());
        handler.handle(eventContext);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 0);

        String jobId = ((AddCuboidEvent) eventContext.getEvent()).getJobId();
        AbstractExecutable job = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId);
        Assert.assertNotNull(job);
        Assert.assertEquals(NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .getSegments().getFirstSegment().getId(),
                ((ChainedExecutable) job).getTasks().get(1).getParam("segmentIds"));
        Assert.assertEquals("20000020001,20000030001,1010001", ((ChainedExecutable) job).getTask(NSparkCubingStep.class).getParam(NBatchConstants.P_LAYOUT_IDS));
    }

    @Test
    public void testHandleEmptySegment() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));

        AddCuboidEvent event = new AddCuboidEvent();
        event.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        EventContext eventContext = new EventContext(event, getTestConfig(), DEFAULT_PROJECT);
        val handler = Mockito.spy(new AddCuboidHandler());
        handler.handle(eventContext);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 0);

        String jobId = ((AddCuboidEvent) eventContext.getEvent()).getJobId();
        AbstractExecutable job = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getJob(jobId);
        Assert.assertNotNull(job);
    }

    private List<Long> calcAddedCuboidLayoutIds(List<NSmartContext.NModelContext> contexts) {
        List<Long> originLayoutIds = new ArrayList<>();
        List<Long> targetLayoutIds = new ArrayList<>();

        NSmartContext.NModelContext context = contexts.get(0);
        IndexPlan originIndexPlan = context.getOrigIndexPlan();
        IndexPlan targetIndexPlan = context.getTargetIndexPlan();
        for (LayoutEntity layout : originIndexPlan.getAllLayouts()) {
            originLayoutIds.add(layout.getId());
        }
        for (LayoutEntity layout : targetIndexPlan.getAllLayouts()) {
            targetLayoutIds.add(layout.getId());
        }

        targetLayoutIds.removeAll(originLayoutIds);

        return targetLayoutIds;
    }

}
