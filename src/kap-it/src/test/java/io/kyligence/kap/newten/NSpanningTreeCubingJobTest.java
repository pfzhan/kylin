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

package io.kyligence.kap.newten;

import java.util.List;
import java.util.Set;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;

public class NSpanningTreeCubingJobTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/spanning_tree_build");
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.spark.cache-threshold", "2");
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    /** 说明
     * index entity: 100000, dimBitSet={0, 1, 2}, measureBitSet={100000}
     * index entity: 10000,  dimBitSet={0, 1}, measureBitSet={100000}
     * index entity: 20000,  dimBitSet={0, 2}, measureBitSet={100000}
     *
     * index entity: 200000, dimBitSet={0, 1, 3, 4}, measureBitSet={100000}
     * index entity: 30000,  dimBitSet={0, 3, 4}, measureBitSet={100000}
     * index entity: 40000,  dimBitSet={0, 3}, measureBitSet={100000}
     * index entity: 0,      dimBitSet={0}, measureBitSet={100000}
     *
     *
     *  最后生成的树:
     *  roots                    1000000                    2000000
     *                            /   \                        |
     *  level1               10000   20000                   30000
     *                                                         |
     *  level2                                               40000
     *                                                         |
     *  level3                                                 0
     */
    @Test
    public void testBuild() {
        final String dataflowId = "75080248-367e-4bac-9fd7-322517ee0227";

        NDataflowManager manager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow dataflow = manager.getDataflow(dataflowId);

        Set<LayoutEntity> layouts = Sets.newLinkedHashSet(dataflow.getIndexPlan().getAllLayouts());
        NSpanningTree spanningTree = NSpanningTreeFactory.fromLayouts(layouts, dataflowId);
        List<IndexEntity> roots = Lists.newArrayList(spanningTree.getRootIndexEntities());
        Assert.assertEquals(2, roots.size());
    }

    @Override
    public String getProject() {
        return "spanning_tree";
    }

}
