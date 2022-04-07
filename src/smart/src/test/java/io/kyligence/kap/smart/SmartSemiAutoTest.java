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

import java.io.File;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class SmartSemiAutoTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testWithSource() throws Exception {
        val project = "default";
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);

        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        modelManager.listAllModelIds().forEach(id -> {
            dataflowManager.dropDataflow(id);
            indexPlanManager.dropIndexPlan(id);
            modelManager.dropModel(id);
        });

        val baseModel = JsonUtil.readValue(new File("src/test/resources/nsmart/default/model_desc/model.json"),
                NDataModel.class);
        baseModel.setProject(project);
        modelManager.createDataModelDesc(baseModel, "ADMIN");
        val indexPlan = new IndexPlan();
        indexPlan.setUuid(baseModel.getUuid());
        indexPlan.setProject(project);
        indexPlanManager.createIndexPlan(indexPlan);
        dataflowManager.createDataflow(indexPlan, "ADMIN");
        dataflowManager.updateDataflowStatus(baseModel.getUuid(), RealizationStatusEnum.ONLINE);

        val sql1 = "select sum(TEST_KYLIN_FACT.ITEM_COUNT) from TEST_KYLIN_FACT group by TEST_KYLIN_FACT.CAL_DT;";
        val sql2 = "select LEAF_CATEG_ID from TEST_KYLIN_FACT;";

        AbstractSemiContext context1 = (AbstractSemiContext) ProposerJob.genOptRec(getTestConfig(), project,
                new String[] { sql1 });
        List<AbstractContext.ModelContext> modelContexts1 = context1.getModelContexts();
        Assert.assertEquals(1, modelContexts1.size());
        AbstractContext.ModelContext modelContext1 = modelContexts1.get(0);
        Assert.assertEquals(1, modelContext1.getIndexRexItemMap().size());

        AbstractSemiContext context2 = (AbstractSemiContext) ProposerJob.genOptRec(getTestConfig(), project,
                new String[] { sql2 });
        List<AbstractContext.ModelContext> modelContexts2 = context2.getModelContexts();
        Assert.assertEquals(1, modelContexts2.size());
        AbstractContext.ModelContext modelContext2 = modelContexts2.get(0);
        Assert.assertEquals(1, modelContext2.getIndexRexItemMap().size());
    }
}
