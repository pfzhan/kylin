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
package io.kyligence.kap.metadata.cube;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;

public class ProjectSpecificConfigTest extends NLocalFileMetadataTestCase {
    public static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testProject1() {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        IndexPlan indexPlan = NIndexPlanManager.getInstance(baseConfig, DEFAULT_PROJECT)
                .getIndexPlanByModelAlias("nmodel_basic");
        verifyProjectOverride(baseConfig, indexPlan.getConfig());
    }

    @Test
    public void testProject2() {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(baseConfig, DEFAULT_PROJECT)
                .getDataflowByModelAlias("nmodel_basic");
        verifyProjectOverride(baseConfig, dataflow.getConfig());
    }

    private void verifyProjectOverride(KylinConfig base, KylinConfig override) {
        assertEquals(3, base.getSlowQueryDefaultDetectIntervalSeconds());
        assertEquals(4, override.getSlowQueryDefaultDetectIntervalSeconds());
    }
}