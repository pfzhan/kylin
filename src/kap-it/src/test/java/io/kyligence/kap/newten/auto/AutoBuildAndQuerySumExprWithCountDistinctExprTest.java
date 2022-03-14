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

package io.kyligence.kap.newten.auto;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoBuildAndQuerySumExprWithCountDistinctExprTest extends AutoTestBase {

    private void fullBuildAllCube(String dfName, String prj) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(round1), true);
    }

    @Test
    public void testUnionAllForSumExprWithCountDistinctExpr() throws Exception {
        String modelId = "73e06974-e642-6b91-e7a0-5cd7f02ec4f2";
        fullBuildAllCube(modelId, getProject());
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");
        executeTestScenario(BuildAndCompareContext.builder() //
                .testScenario(new TestScenario(CompareLevel.SAME, "query/sql_sum_expr_with_count_distinct_expr")) //
                .isCompareLayout(false) //
                .build());
    }

    @Override
    public String getProject() {
        return "sum_expr_with_count_distinct";
    }

}
