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
//package io.kyligence.kap.rest.service.handler;
//
//import java.util.List;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import com.google.common.collect.Lists;
//
//import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
//import io.kyligence.kap.cube.model.NCubePlan;
//import io.kyligence.kap.cube.model.NCubePlanManager;
//import io.kyligence.kap.cube.model.NCuboidLayout;
//import io.kyligence.kap.event.manager.EventDao;
//import io.kyligence.kap.event.model.AccelerateEvent;
//import io.kyligence.kap.event.model.Event;
//import io.kyligence.kap.event.model.EventContext;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//public class AccelerateEventHandlerTest extends NLocalFileMetadataTestCase {
//
//    private static final String DEFAULT_PROJECT = "default";
//    @Before
//    public void setUp() throws Exception {
//        this.createTestMetadata();
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        this.cleanupTestMetadata();
//    }
//
//    @Test
//    @Ignore("reopen it after #8219")
//    public void testHandlerIdempotent() throws Exception {
//
//        getTestConfig().setProperty("kylin.server.mode", "query");
//
//        AccelerateEvent event = new AccelerateEvent();
//        event.setModels(Lists.newArrayList());
//        event.setFavoriteMark(true);
//        event.setProject(DEFAULT_PROJECT);
//        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
//        NCubePlan cubePlan1 = cubePlanManager.getCubePlan("all_fixed_length");
//        logLayouts(cubePlan1);
//        int layoutCount1 = cubePlan1.getAllCuboidLayouts().size();
//
//        event.setSqlPatterns(Lists.newArrayList("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME"));
//        EventContext eventContext = new EventContext(event, getTestConfig());
//        AccelerateEventHandler handler = new AccelerateEventHandler();
//        // add favorite sql to update model and post an new AddCuboidEvent
//        handler.handle(eventContext);
//
//        NCubePlan cubePlan2 = cubePlanManager.getCubePlan("all_fixed_length");
//        logLayouts(cubePlan2);
//        int layoutCount2 = cubePlan2.getAllCuboidLayouts().size();
//        Assert.assertEquals(layoutCount1 + 1, layoutCount2);
//
//        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
//        Assert.assertNotNull(events);
//        Assert.assertEquals(2, events.size());
//
//        // run again, and model will not update and will not post an new AddCuboidEvent
//        handler.handle(eventContext);
//
//        NCubePlan cubePlan3 = cubePlanManager.getCubePlan("all_fixed_length");
//        int layoutCount3 = cubePlan3.getAllCuboidLayouts().size();
//        Assert.assertEquals(layoutCount3, layoutCount2);
//
//        // cancel favorite sql will not update model and cubePlan, just post an new RemoveCuboidEvent
//        event.setFavoriteMark(false);
//        handler.handle(eventContext);
//
//        NCubePlan cubePlan4 = cubePlanManager.getCubePlan("all_fixed_length");
//        int layoutCount4 = cubePlan4.getAllCuboidLayouts().size();
//        Assert.assertEquals(layoutCount3, layoutCount4);
//
//        events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
//        Assert.assertNotNull(events);
//        Assert.assertEquals(3, events.size());
//
//        getTestConfig().setProperty("kylin.server.mode", "all");
//
//    }
//
//    private void logLayouts(NCubePlan cubePlan) {
//        for (NCuboidLayout layout : cubePlan.getAllCuboidLayouts()) {
//            log.debug("layout id:{} -- {}, auto:{}, manual:{}, col:{}, sort:{}", layout.getId(),
//                    layout.getCuboidDesc().getId(), layout.isAuto(), layout.isManual(), layout.getColOrder(),
//                    layout.getSortByColumns());
//        }
//    }
//
//
//}

