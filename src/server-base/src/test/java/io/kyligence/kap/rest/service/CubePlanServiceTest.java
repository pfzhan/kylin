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
package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.rest.constant.Constant;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import lombok.val;

public class CubePlanServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private CubePlanService modelService = Mockito.spy(new CubePlanService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDown() {
        cleanAfterClass();
    }

    @Before
    public void setup() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

    }

    @Test
    public void testUpdateCubePlan() throws IOException, PersistentException {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ut_inner_join_cube_partial");
        val saved = modelService.updateRuleBasedCuboid(UpdateRuleBasedCuboidRequest.builder()
                .project("default")
                .cubePlanName("ut_inner_join_cube_partial")
                .dimensions(Arrays.asList(1, 2, 3, 4))
                .measures(Arrays.asList(1001, 1002))
                .aggregationGroups(Lists.<NAggregationGroup>newArrayList())
                .build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(origin.getAllCuboidLayouts().size() + 1, saved.getAllCuboidLayouts().size());
    }

    @Test
    public void testUpdateEmptyRule() throws IOException, PersistentException {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val origin = cubePlanManager.getCubePlan("ncube_basic");
        val saved = modelService.updateRuleBasedCuboid(UpdateRuleBasedCuboidRequest.builder()
                .project("default")
                .cubePlanName("ncube_basic")
                .dimensions(Arrays.asList(1, 2, 3, 4))
                .measures(Arrays.asList(1001, 1002))
                .aggregationGroups(Lists.<NAggregationGroup>newArrayList())
                .build());
        Assert.assertNotNull(saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid());
        Assert.assertEquals(4, saved.getRuleBasedCuboidsDesc().getNewRuleBasedCuboid().getDimensions().size());
        Assert.assertEquals(origin.getAllCuboidLayouts().size() + 1, saved.getAllCuboidLayouts().size());
    }
}
