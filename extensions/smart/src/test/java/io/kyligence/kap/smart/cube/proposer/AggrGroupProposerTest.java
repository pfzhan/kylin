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

package io.kyligence.kap.smart.cube.proposer;

import java.util.ArrayList;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.cube.CubeContextBuilder;
import io.kyligence.kap.smart.query.Utils;

public class AggrGroupProposerTest {
    private static KylinConfig kylinConfig = Utils.newKylinConfig("src/test/resources/learn_kylin/meta");

    @BeforeClass
    public static void beforeClass() {
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
    }

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testOnStarModel() throws JsonProcessingException {
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model_star");
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, new String[0]);
        CubeDesc initCubeDesc = context.getDomain().buildCubeDesc();
        AggrGroupProposer proposer = new AggrGroupProposer(context);
        CubeDesc newCubeDesc = proposer.propose(initCubeDesc);
        newCubeDesc.init(kylinConfig);
        newCubeDesc.validateAggregationGroups();

        SelectRule rule = newCubeDesc.getAggregationGroups().get(0).getSelectRule();
        Assert.assertTrue(rule.hierarchyDims.length + rule.jointDims.length + rule.mandatoryDims.length > 0);
    }

    @Test
    public void testOnEmptyCube() throws JsonProcessingException {
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model_star");
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, new String[0]);
        CubeDesc initCubeDesc = context.getDomain().buildCubeDesc();
        initCubeDesc.setAggregationGroups(new ArrayList<AggregationGroup>(0));

        AggrGroupProposer proposer = new AggrGroupProposer(context);
        CubeDesc newCubeDesc = proposer.propose(initCubeDesc);
        newCubeDesc.init(kylinConfig);
        newCubeDesc.validateAggregationGroups();

        SelectRule rule = newCubeDesc.getAggregationGroups().get(0).getSelectRule();
        Assert.assertTrue(rule.hierarchyDims.length + rule.jointDims.length + rule.mandatoryDims.length > 0);
    }

    @Test
    public void testOnSnowModel() throws JsonProcessingException {
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model");
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, new String[0]);
        CubeDesc initCubeDesc = context.getDomain().buildCubeDesc();
        AggrGroupProposer proposer = new AggrGroupProposer(context);
        CubeDesc newCubeDesc = proposer.propose(initCubeDesc);
        newCubeDesc.init(kylinConfig);
        newCubeDesc.validateAggregationGroups();

        SelectRule rule = newCubeDesc.getAggregationGroups().get(0).getSelectRule();
        Assert.assertTrue(rule.hierarchyDims.length + rule.jointDims.length + rule.mandatoryDims.length > 0);
    }

    @Test
    public void testOnSnowModelWithSQL() throws JsonProcessingException {
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model");
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc,
                new String[] { "select count(*) from kylin_sales" });
        CubeDesc initCubeDesc = context.getDomain().buildCubeDesc();
        AggrGroupProposer proposer = new AggrGroupProposer(context);
        CubeDesc newCubeDesc = proposer.propose(initCubeDesc);
        newCubeDesc.init(kylinConfig);
        newCubeDesc.validateAggregationGroups();

        SelectRule rule = newCubeDesc.getAggregationGroups().get(0).getSelectRule();
        Assert.assertTrue(rule.hierarchyDims.length + rule.jointDims.length + rule.mandatoryDims.length > 0);
    }
}
