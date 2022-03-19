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
package io.kyligence.kap.smart.model;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelCreateContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class ModelRenameProposerTest extends NLocalWithSparkSessionTest {
    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testAutoRenameWithOfflineModel() {
        String[] accSql1 = {"select count(*)  FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID"};
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare initial model
        val smartContext = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(), accSql1);
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // offline model
        AbstractContext context = ProposerJob.propose(new ModelCreateContext(getTestConfig(), getProject(), accSql1));
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(modelContext.getTargetModel().getAlias(), "AUTO_MODEL_TEST_ORDER_2");

        //online model
        AccelerationContextUtil.onlineModel(smartContext);
        context = ProposerJob.propose(new ModelCreateContext(getTestConfig(), getProject(), accSql1));
        modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(modelContext.getTargetModel().getAlias(), "AUTO_MODEL_TEST_ORDER_2");

        // broken model
        NTableMetadataManager.getInstance(getTestConfig(), getProject()).removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        String[] accSql2 = { "select count(*)  FROM TEST_ORDER" };

        context = ProposerJob.propose(new ModelCreateContext(getTestConfig(), getProject(), accSql2));
        modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(modelContext.getTargetModel().getAlias(), "AUTO_MODEL_TEST_ORDER_2");

    }

}
