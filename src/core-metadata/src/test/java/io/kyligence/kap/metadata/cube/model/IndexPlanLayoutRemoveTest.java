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

package io.kyligence.kap.metadata.cube.model;

import com.google.common.collect.Sets;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexPlanLayoutRemoveTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/flexible_seg_index");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRemoveLayout() {
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan indexPlan = indexMgr.getIndexPlanByModelAlias("model1");
        Assert.assertNotNull(indexPlan);

        NDataflowManager dataflowMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);

        Assert.assertEquals(1, dataflowMgr.getDataflow(indexPlan.getId()).getSegment("14fcab15-09a4-4308-a604-1735693dc373").getLayoutSize());
        Assert.assertEquals(1, dataflowMgr.getDataflow(indexPlan.getId()).getSegment("fe0afd9e-97d4-47c5-b439-883d0921afa1").getLayoutSize());

        indexMgr.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(20000000001L), true, true);
        });
        Assert.assertEquals(0, dataflowMgr.getDataflow(indexPlan.getId()).getSegment("14fcab15-09a4-4308-a604-1735693dc373").getLayoutSize());
        Assert.assertEquals(1, dataflowMgr.getDataflow(indexPlan.getId()).getSegment("fe0afd9e-97d4-47c5-b439-883d0921afa1").getLayoutSize());

        indexMgr.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), true, true);
        });
        Assert.assertEquals(0, dataflowMgr.getDataflow(indexPlan.getId()).getSegment("14fcab15-09a4-4308-a604-1735693dc373").getLayoutSize());
        Assert.assertEquals(0, dataflowMgr.getDataflow(indexPlan.getId()).getSegment("fe0afd9e-97d4-47c5-b439-883d0921afa1").getLayoutSize());
    }
}
