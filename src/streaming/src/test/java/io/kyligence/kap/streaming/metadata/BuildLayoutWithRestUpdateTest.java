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
package io.kyligence.kap.streaming.metadata;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public class BuildLayoutWithRestUpdateTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testUpdateLayouts() {
        val layoutUpdater = new BuildLayoutWithRestUpdate(JobTypeEnum.STREAMING_BUILD) {
            public void updateLayouts(KylinConfig config, String project, String dataflowId,
                    final List<NDataLayout> layouts) {
                super.updateLayouts(config, project, dataflowId, layouts);
            }
        };

        val segId = "c380dd2a-43b8-4268-b73d-2a5f76236633";
        val dataflowId = "e78a89dd-847f-4574-8afa-8768b4228b72";
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(dataflowId);
        val seg = df.getSegment(segId);
        Assert.assertEquals(17, seg.getLayoutSize());
        val layouts = new ArrayList<NDataLayout>();
        layouts.add(NDataLayout.newDataLayout(df, seg.getId(), 10002L));
        layoutUpdater.updateLayouts(testConfig, PROJECT, dataflowId, layouts);
        NDataflowManager mgr1 = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df1 = mgr1.getDataflow(dataflowId);
        val seg1 = df1.getSegment(segId);
        Assert.assertEquals(18, seg1.getLayoutSize());

    }
}
