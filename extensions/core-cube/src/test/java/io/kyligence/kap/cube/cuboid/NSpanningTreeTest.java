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

package io.kyligence.kap.cube.cuboid;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;

public class NSpanningTreeTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        NCubePlanManager mgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cube = mgr.getCubePlan("62b3c058-5514-436b-b6b5-6240a8d91108");
        Assert.assertNotNull(cube);

        NSpanningTree spanningTree = cube.getSpanningTree();
        Assert.assertTrue(spanningTree instanceof NForestSpanningTree);
        Assert.assertNotNull(spanningTree);

        Assert.assertEquals(cube.getCuboids().size(), spanningTree.getCuboidCount());
        Assert.assertEquals(cube.getCuboids().size(), spanningTree.getAllCuboidDescs().size());
        Assert.assertEquals(3, spanningTree.getBuildLevel());
        Assert.assertEquals(cube.getName(), spanningTree.getCuboidCacheKey());

        NCuboidDesc cuboidDesc = spanningTree.getCuboidDesc(1000L);
        Assert.assertNotNull(cuboidDesc);
        Assert.assertTrue(cube.getCuboids().contains(cuboidDesc));
        Assert.assertEquals(2, cuboidDesc.getLayouts().size());

        NCuboidLayout cuboidLayout = spanningTree.getCuboidLayout(1001L);
        Assert.assertNotNull(cuboidLayout);
        Assert.assertSame(spanningTree.getCuboidDesc(1000L).getLayouts().get(0), cuboidLayout);

        NCuboidDesc childCuboid1 = spanningTree.getCuboidDesc(0L);
        NCuboidDesc childCuboid2 = spanningTree.getCuboidDesc(1000L);
        NCuboidDesc childCuboid3 = spanningTree.getCuboidDesc(2000L);
        NCuboidDesc rootCuboid = spanningTree.getCuboidDesc(3000L);

        Assert.assertEquals(1, spanningTree.getRootCuboidDescs().size());
        Assert.assertTrue(spanningTree.getRootCuboidDescs().contains(rootCuboid));
        Assert.assertSame(rootCuboid, spanningTree.getRootCuboidDesc(childCuboid1));
        Assert.assertSame(rootCuboid, spanningTree.getParentCuboidDesc(childCuboid2));
        Assert.assertSame(null, spanningTree.getParentCuboidDesc(rootCuboid));
        Assert.assertEquals(4, spanningTree.retrieveAllMeasures(rootCuboid).size());
        Assert.assertTrue(spanningTree.getSpanningCuboidDescs(rootCuboid).contains(childCuboid2));
    }

    @Test
    public void testTransverse() {
        NCubePlanManager mgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cube = mgr.getCubePlan("62b3c058-5514-436b-b6b5-6240a8d91108");
        Assert.assertNotNull(cube);

        NSpanningTree spanningTree = cube.getSpanningTree();
        CounterTreeVisitor visitor = new CounterTreeVisitor();
        NCuboidLayout matched = spanningTree.findBestMatching(visitor);
        Assert.assertEquals(spanningTree.getCuboidCount(), visitor.getCnt());
        Assert.assertNotNull(matched);
    }

    private static class CounterTreeVisitor implements NSpanningTree.ICuboidTreeVisitor {
        int cnt = 0;
        List<NCuboidLayout> matched = Lists.newLinkedList();

        @Override
        public boolean visit(NCuboidDesc cuboidDesc) {
            cnt++;
            matched.addAll(cuboidDesc.getLayouts());
            return true;
        }

        @Override
        public NCuboidLayout getMatched() {
            return matched.get(0);
        }

        private int getCnt() {
            return cnt;
        }
    }
}
