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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kylingence.kap.event.manager.EventDao;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.EventContext;
import io.kylingence.kap.event.model.ModelUpdateEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class ModelUpdateHandlerTest extends NLocalFileMetadataTestCase {

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
    public void testHandler() throws Exception {
        ModelUpdateEvent event = new ModelUpdateEvent();
        event.setFavoriteMark(true);
        event.setProject(DEFAULT_PROJECT);
        int layoutCount = 0;
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NCubePlan cubePlan1 = cubePlanManager.getCubePlan("all_fixed_length");
        layoutCount += cubePlan1.getAllCuboidLayouts().size();
        NCubePlan cubePlan2 = cubePlanManager.getCubePlan("ncube_basic");
        layoutCount += cubePlan2.getAllCuboidLayouts().size();

        event.setSqlMap(new HashMap<String, Long>(){{put("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME", 0L);}});
        EventContext eventContext = new EventContext(event, getTestConfig());
        ModelUpdateHandler handler = new ModelUpdateHandler();
        handler.handle(eventContext);


        int newLayoutCount = 0;
        NCubePlan cubePlan3 = cubePlanManager.getCubePlan("all_fixed_length");
        newLayoutCount += cubePlan3.getAllCuboidLayouts().size();
        NCubePlan cubePlan4 = cubePlanManager.getCubePlan("ncube_basic");
        newLayoutCount += cubePlan4.getAllCuboidLayouts().size();

        Assert.assertEquals(layoutCount + 1, newLayoutCount);

        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 2);
    }

}
