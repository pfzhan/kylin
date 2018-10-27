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

import java.util.Arrays;

import io.kyligence.kap.event.model.EventContext;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.event.model.PostCubePlanRuleUpdateEvent;
import lombok.val;

public class PostCubePlanRuleUpdateHandlerTest extends NLocalFileMetadataTestCase {
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
    public void testCleanUp() throws Exception {
        val cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), "default");
        val cubePlanCleanupEvent = new PostCubePlanRuleUpdateEvent();

        val cubePlan = cubePlanManager.updateCubePlan("ncube_basic_inner", new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                val newRule = new NRuleBasedCuboidsDesc();
                newRule.setDimensions(Arrays.asList(1, 2, 3, 4, 5, 6));
                newRule.setMeasures(Arrays.asList(1001, 1002));
                copyForWrite.getRuleBasedCuboidsDesc().setNewRuleBasedCuboid(newRule);
            }
        });
        cubePlanCleanupEvent.setProject("default");
        cubePlanCleanupEvent.setCubePlanName(cubePlan.getName());

        val eventContext = new EventContext(cubePlanCleanupEvent, getTestConfig());
        val handler = new PostCubePlanRuleUpdateHandler();
        handler.handle(eventContext);

        val cleanedPlan = cubePlanManager.getCubePlan("ncube_basic_inner");
        Assert.assertNull(cleanedPlan.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertThat(cleanedPlan.getRuleBasedCuboidsDesc().getDimensions(), CoreMatchers.is(Arrays.asList(1, 2, 3, 4, 5, 6)));
        Assert.assertEquals(1, cleanedPlan.getRuleBaseCuboidLayouts().size());
    }

}