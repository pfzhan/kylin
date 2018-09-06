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


import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.smart.NSmartMaster;
import io.kylingence.kap.event.model.RemoveCuboidEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kylingence.kap.event.model.EventContext;

import java.util.List;

public class RemoveCuboidHandlerTest extends NLocalFileMetadataTestCase {

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

        // first add cuboid layouts
        List<String> sqls = Lists.<String>newArrayList("select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT");
        NSmartMaster master = new NSmartMaster(getTestConfig(), DEFAULT_PROJECT, sqls.toArray(new String[0]));
        master.runAll();

        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NCubePlan cubePlan = cubePlanManager.getCubePlan("all_fixed_length");
        int cuboidLayoutSize = cubePlan.getAllCuboidLayouts().size();

        RemoveCuboidEvent event = new RemoveCuboidEvent();
        event.setApproved(true);
        event.setProject(DEFAULT_PROJECT);
        event.setModelName("nmodel_basic");
        event.setCubePlanName("all_fixed_length");
        event.setSqlList(sqls);

        EventContext eventContext = new EventContext(event, getTestConfig());
        RemoveCuboidHandler handler = new RemoveCuboidHandler();
        // then remove the added cuboid layouts
        handler.handle(eventContext);

        cubePlan = cubePlanManager.getCubePlan("all_fixed_length");
        int cuboidLayoutSize2 = cubePlan.getAllCuboidLayouts().size();

        Assert.assertEquals(cuboidLayoutSize - 1, cuboidLayoutSize2);

        // handle again, will not reduce dataFlow's cuboidLayouts
        handler.handle(eventContext);

        cubePlan = cubePlanManager.getCubePlan("all_fixed_length");
        int cuboidLayoutSize3 = cubePlan.getAllCuboidLayouts().size();

        Assert.assertEquals(cuboidLayoutSize3, cuboidLayoutSize2);

        getTestConfig().setProperty("kylin.server.mode", "all");

    }

}
