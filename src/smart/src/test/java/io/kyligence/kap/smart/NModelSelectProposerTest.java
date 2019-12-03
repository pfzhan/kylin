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

package io.kyligence.kap.smart;


import java.util.List;

import org.apache.calcite.sql.SqlSelect;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.util.QueryAliasMatcher;
import io.kyligence.kap.smart.model.ModelTree;

public class NModelSelectProposerTest extends NLocalWithSparkSessionTest {

    @After
    public void tearDown() {
        restoreAllSystemProp();
    }

    @Test
    public void testInnerJoinProposerInAutoMode() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        // proposer and save A inner join B inner join C
        String[] sql1 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " +
                "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID"};
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sql1);
        smartMaster1.runAll();
        smartMaster1.saveModelOnlyForTest();

        // in smartMode, proposer A inner join B, will not reuse origin A inner join B inner join C model
        // so it will proposer a new model that A inner join B.
        String[] sql2 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "};
        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), "newten", sql2);
        smartMaster2.runAll();
        smartMaster2.saveModelOnlyForTest();
        List<NDataModel> recommendedModels = smartMaster2.getRecommendedModels();
        Assert.assertTrue(recommendedModels.size() == 1);
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().size() == 1);
        Assert.assertTrue(recommendedModels.get(0).getRootFactTable().getAlias().equals("TEST_KYLIN_FACT"));
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().get(0).getTable().equals("DEFAULT.TEST_ORDER"));
    }

    @Test
    public void testInnerJoinProposerInSemiAutoMode() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        // proposer and save A inner join B inner join C
        String[] sql1 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " +
                "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID"};
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sql1);
        smartMaster1.runAll();
        smartMaster1.saveModelOnlyForTest();

        // in semiAutoMode, proposer A inner join B, will reuse origin A inner join B inner join C model
        // so it will not proposer a new model
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject("newten"));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);
        getTestConfig().setProperty("kap.metadata.semi-automatic-mode", "true");// set model maintain type to semi-auto

        String[] sql2 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "};
        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), "newten", sql2);
        smartMaster2.runAll();
        smartMaster2.saveModelOnlyForTest();
        List<NDataModel> recommendedModels = smartMaster2.getRecommendedModels();
        Assert.assertTrue(recommendedModels.size() == 1);
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().size() == 2);
        Assert.assertTrue(recommendedModels.get(0).getRootFactTable().getAlias().equals("TEST_KYLIN_FACT"));
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().get(0).getTable().equals("DEFAULT.TEST_ACCOUNT"));
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().get(1).getTable().equals("DEFAULT.TEST_ORDER"));
    }

    @Test
    public void testInnerJoinProposerInExpertMode() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        // proposer and save A inner join B inner join C
        String[] sql1 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " +
                "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID"};
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sql1);
        smartMaster1.runAll();
        smartMaster1.saveModelOnlyForTest();

        // in expertMode, proposer A inner join B, will reuse origin A inner join B inner join C model
        // so it will not proposer a new model
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject("newten"));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate); // set model maintain type to ExpertMode

        String[] sql2 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "};
        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), "newten", sql2);
        smartMaster2.runAll();
        smartMaster2.saveModelOnlyForTest();
        List<NDataModel> recommendedModels = smartMaster2.getRecommendedModels();
        Assert.assertTrue(recommendedModels.size() == 1);
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().size() == 2);
        Assert.assertTrue(recommendedModels.get(0).getRootFactTable().getAlias().equals("TEST_KYLIN_FACT"));
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().get(0).getTable().equals("DEFAULT.TEST_ACCOUNT"));
        Assert.assertTrue(recommendedModels.get(0).getJoinTables().get(1).getTable().equals("DEFAULT.TEST_ORDER"));
    }

    @Test
    public void testMatchModelTreeWhenDisablePartialMatch() {
        // A inner join B inner join C
        String[] sql1 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " +
                "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID"};
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sql1);
        smartMaster1.runAll();
        NDataModel dataModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();

        // A inner join B
        String[] sql2 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "};
        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), "newten", sql2);
        smartMaster2.runAll();
        ModelTree modelTree = smartMaster2.getContext().getModelContexts().get(0).getModelTree();

        // A <-> B can't match A <-> B <-> C in smartMode, when disable partial match
        Assert.assertFalse(NModelSelectProposer.matchModelTree(dataModel, modelTree, true));

        // A <-> B can't match A <-> B <-> C in expertMode and semiAutoMode, when disable partial match
        Assert.assertFalse(NModelSelectProposer.matchModelTree(dataModel, modelTree, false));
    }

    @Test
    public void testMatchModelTreeEnablePartialMatch() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");

        // A inner join B inner join C
        String[] sql1 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " +
                "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID"};
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sql1);
        smartMaster1.runAll();
        NDataModel dataModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();

        // A inner join B
        String[] sql2 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "};
        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), "newten", sql2);
        smartMaster2.runAll();
        ModelTree modelTree = smartMaster2.getContext().getModelContexts().get(0).getModelTree();

        // A <-> B can't match A <-> B <-> C in smartMode, when enable partial match
        Assert.assertFalse(NModelSelectProposer.matchModelTree(dataModel, modelTree, true));

        // A <-> B can match A <-> B <-> C in expertMode and semiAutoMode, when enable partial match
        Assert.assertTrue(NModelSelectProposer.matchModelTree(dataModel, modelTree, false));
    }

    @Test
    public void testInnerJoinCCReplaceError() throws Exception {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");

        String[] sql1 = new String[] {"select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " +
                "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID"};
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sql1);
        smartMaster1.runAll();
        NDataModel dataModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();

        QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher("newten", "DEFAULT");
        SqlSelect sql2 = (SqlSelect) CalciteParser.parse("select * from test_kylin_fact " +
                "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID ");

        // before fix, the match result is empty， when enable partial match
        Assert.assertFalse(queryAliasMatcher.match(dataModel, sql2).getAliasMapping().isEmpty());
    }
}