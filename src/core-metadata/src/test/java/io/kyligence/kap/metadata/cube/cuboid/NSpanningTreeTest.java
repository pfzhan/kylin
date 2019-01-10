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

package io.kyligence.kap.metadata.cube.cuboid;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;

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
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        Assert.assertNotNull(cube);

        NSpanningTree spanningTree = cube.getSpanningTree();
        Assert.assertTrue(spanningTree instanceof NForestSpanningTree);
        Assert.assertNotNull(spanningTree);

        Assert.assertEquals(cube.getAllIndexes().size(), spanningTree.getCuboidCount());
        Assert.assertEquals(cube.getAllIndexes().size(), spanningTree.getAllIndexEntities().size());
        Assert.assertEquals(4, spanningTree.getBuildLevel());
        Assert.assertEquals(cube.getUuid(), spanningTree.getCuboidCacheKey());

        IndexEntity cuboidDesc = spanningTree.getIndexEntity(10000L);
        Assert.assertNotNull(cuboidDesc);
        Assert.assertTrue(cube.getAllIndexes().contains(cuboidDesc));
        Assert.assertEquals(2, cuboidDesc.getLayouts().size());

        LayoutEntity cuboidLayout = spanningTree.getCuboidLayout(10001L);
        Assert.assertNotNull(cuboidLayout);
        Assert.assertSame(spanningTree.getIndexEntity(10000L).getLayouts().get(0), cuboidLayout);

        IndexEntity childCuboid1 = spanningTree.getIndexEntity(0L);
        IndexEntity childCuboid2 = spanningTree.getIndexEntity(20000L);
        IndexEntity rootCuboid = spanningTree.getIndexEntity(30000L);
        IndexEntity cubeCuboidRoot = spanningTree.getIndexEntity(1000000L);
        IndexEntity tableIndexCuboidRoot = spanningTree.getIndexEntity(20000020000L);

        Assert.assertEquals(2, spanningTree.getRootIndexEntities().size());
        Assert.assertTrue(spanningTree.getRootIndexEntities().contains(tableIndexCuboidRoot));
        Assert.assertTrue(spanningTree.getRootIndexEntities().contains(cubeCuboidRoot));
        Assert.assertSame(cubeCuboidRoot, spanningTree.getRootIndexEntity(childCuboid1));
        Assert.assertSame(cubeCuboidRoot, spanningTree.getParentIndexEntity(rootCuboid));
        Assert.assertSame(rootCuboid, spanningTree.getParentIndexEntity(childCuboid2));
        Assert.assertSame(null, spanningTree.getParentIndexEntity(cubeCuboidRoot));
        Assert.assertEquals(6, spanningTree.retrieveAllMeasures(rootCuboid).size());
        Assert.assertEquals(16, spanningTree.retrieveAllMeasures(cubeCuboidRoot).size());
        Assert.assertTrue(spanningTree.getSpanningIndexEntities(rootCuboid).contains(childCuboid2));
    }

    @Test
    public void testTransverse() {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        Assert.assertNotNull(cube);

        NSpanningTree spanningTree = cube.getSpanningTree();
        CounterTreeVisitor visitor = new CounterTreeVisitor();
        spanningTree.acceptVisitor(visitor);
        LayoutEntity matched = visitor.getBestLayoutCandidate().getCuboidLayout();

        Assert.assertEquals(spanningTree.getCuboidCount(), visitor.getCnt());
        Assert.assertNotNull(matched);
    }

    private static class CounterTreeVisitor implements NSpanningTree.ISpanningTreeVisitor {
        int cnt = 0;
        List<LayoutEntity> matched = Lists.newLinkedList();

        @Override
        public boolean visit(IndexEntity cuboidDesc) {
            cnt++;
            matched.addAll(cuboidDesc.getLayouts());
            return true;
        }

        @Override
        public NLayoutCandidate getBestLayoutCandidate() {
            return new NLayoutCandidate(matched.get(0));
        }

        private int getCnt() {
            return cnt;
        }
    }
}
