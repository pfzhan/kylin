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

import java.util.Arrays;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kylingence.kap.event.manager.EventDao;
import io.kylingence.kap.event.model.AddCuboidEvent;
import io.kylingence.kap.event.model.CubePlanUpdateEvent;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.EventContext;
import io.kylingence.kap.event.model.RemoveCuboidEvent;
import lombok.val;

public class CubePlanUpdateHandlerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.server.mode", "query");
    }

    @After
    public void tearDown() throws Exception {
        getTestConfig().setProperty("kylin.server.mode", "all");
        cleanupTestMetadata();
    }

    @Test
    public void testOnlyRuleChanged() throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        val cubePlanUpdateEvent = new CubePlanUpdateEvent();

        val cubePlan = cubePlanManager.updateCubePlan("ncube_basic_inner", new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                val newRule = new NRuleBasedCuboidsDesc();
                newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
                newRule.setMeasures(Arrays.asList(1001, 1002));
                copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
            }
        });
        cubePlanUpdateEvent.setProject("default");
        cubePlanUpdateEvent.setCubePlanName(cubePlan.getName());

        val eventContext = new EventContext(cubePlanUpdateEvent, getTestConfig());
        val handler = new CubePlanUpdateHandler();
        handler.handle(eventContext);

        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        Assert.assertEquals(4, events.size());
        for (Event event : events) {
            if (event instanceof AddCuboidEvent) {
                Assert.assertEquals(1, ((AddCuboidEvent) event).getLayoutIds().size());
            }
            if (event instanceof RemoveCuboidEvent) {
                Assert.assertEquals(13, ((RemoveCuboidEvent) event).getLayoutIds().size());
            }
        }
    }

    @Test
    public void testTableIndexChange() {

    }
}