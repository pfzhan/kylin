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

import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;

public class AutoTpchTest extends AutoTestBase {

    //KAP#7892 fix this
    @Test
    public void testTpch() throws Exception {
        // split batch to verify KAP#9114
        new TestScenario(CompareLevel.SAME, "sql_tpch", 0, 5).execute();
        new TestScenario(CompareLevel.SAME, "sql_tpch", 5, 10).execute();
        new TestScenario(CompareLevel.SAME, "sql_tpch", 10, 15).execute();
        // ignore for KE-16343
        // new TestScenario(CompareLevel.SAME, "sql_tpch", 15, 16).execute();
        new TestScenario(CompareLevel.SAME, "sql_tpch", 16, 22).execute();
    }

    @Test
    public void testReProposeCase() throws Exception {
        // run twice to verify KAP#7515
        for (int i = 0; i < 2; ++i) {
            new TestScenario(CompareLevel.SAME, "sql_tpch", 1, 2).execute();
        }
    }

    @Test
    public void testBatchProposeSQLAndReuseLeftJoinModel() throws Exception {
        // 1st round, recommend model with a single fact table
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        SmartMaster smartMaster1 = proposeWithSmartMaster(getProject(), Lists.newArrayList(
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, false, "sql_tpch/sql_tpch_reprosal/", 0, 1, null)));
        Assert.assertEquals(1, smartMaster1.getContext().getAccelerateInfoMap().size());
        Set<NDataModel> selectedDataModels1 = Sets.newHashSet();
        smartMaster1.getContext().getAccelerateInfoMap().forEach((s, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isFailed());
            accelerateInfo.getRelatedLayouts()
                    .forEach(layout -> selectedDataModels1.add(dataModelManager.getDataModelDesc(layout.getModelId())));
        });
        NDataModel proposedModel1 = selectedDataModels1.iterator().next();
        Assert.assertEquals(0, proposedModel1.getJoinTables().size());
        JoinsGraph graph1 = proposedModel1.getJoinsGraph();

        // 2nd round, reuse the model and increase more Joins which is through accelerating 2 different olapCtx
        SmartMaster smartMaster2 = proposeWithSmartMaster(getProject(), Lists.newArrayList(
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, false, "sql_tpch/sql_tpch_reprosal/", 1, 3, null)));
        Set<NDataModel> selectedDataModels2 = Sets.newHashSet();
        smartMaster2.getContext().getAccelerateInfoMap().forEach((s, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isFailed());
            accelerateInfo.getRelatedLayouts()
                    .forEach(layout -> selectedDataModels2.add(dataModelManager.getDataModelDesc(layout.getModelId())));
        });
        Assert.assertEquals(1, selectedDataModels2.size());
        NDataModel proposedModel2 = selectedDataModels2.iterator().next();
        Assert.assertEquals(6, proposedModel2.getJoinTables().size());
        JoinsGraph graph2 = proposedModel2.getJoinsGraph();
        Assert.assertTrue(graph1.match(graph2, new HashMap<String, String>()));

        // 3rd round, accelerate a sql that its join info equaled with current model, so it won't change previous model
        SmartMaster smartMaster3 = proposeWithSmartMaster(getProject(), Lists.newArrayList(
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, false, "sql_tpch/sql_tpch_reprosal/", 3, 4, null)));
        Set<NDataModel> selectedDataModels3 = Sets.newHashSet();
        smartMaster3.getContext().getAccelerateInfoMap().forEach((s, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isFailed());
            accelerateInfo.getRelatedLayouts()
                    .forEach(layout -> selectedDataModels3.add(dataModelManager.getDataModelDesc(layout.getModelId())));
        });
        Assert.assertEquals(1, selectedDataModels3.size());
        NDataModel proposedModel3 = selectedDataModels3.iterator().next();
        Assert.assertEquals(6, proposedModel3.getJoinTables().size());
        JoinsGraph graph3 = proposedModel3.getJoinsGraph();
        Assert.assertTrue(graph2.match(graph3, new HashMap<>()));
        Assert.assertTrue(graph3.match(graph2, new HashMap<>()));
    }

    @Test
    public void testBatchProposeSQLAndReuseInnerJoinModel() throws Exception {
        //1st round, propose initial model
        SmartMaster smartMaster = proposeWithSmartMaster(getProject(),
                Lists.newArrayList(new TestScenario(CompareLevel.SAME, "sql_tpch")));
        NDataModel originModel = smartMaster.getContext().getModelContexts().stream()
                .filter(ctx -> ctx.getTargetModel().getJoinTables().size() == 6).collect(Collectors.toList()).get(0)
                .getTargetModel();
        JoinsGraph originJoinGragh = originModel.getJoinsGraph();

        SmartMaster smartMaster1 = proposeWithSmartMaster(getProject(),
                Lists.newArrayList(new TestScenario(CompareLevel.SAME, "sql_tpch/sql_tpch_reprosal/", 3, 4)));
        AccelerateInfo accelerateInfo = smartMaster1.getContext().getAccelerateInfoMap().values()
                .toArray(new AccelerateInfo[] {})[0];
        Assert.assertFalse(accelerateInfo.isFailed());

        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataModel dataModel = modelManager
                .getDataModelDesc(Lists.newArrayList(accelerateInfo.getRelatedLayouts()).get(0).getModelId());
        JoinsGraph accelerateJoinGragh = dataModel.getJoinsGraph();
        Assert.assertTrue(originJoinGragh.match(accelerateJoinGragh, new HashMap<>()));
        Assert.assertTrue(accelerateJoinGragh.match(originJoinGragh, new HashMap<>()));
    }

    @Test
    @Ignore
    public void testTemp() throws Exception {
        new TestScenario(CompareLevel.SAME, "query/temp").execute();

    }
}
