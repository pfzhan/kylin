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

package io.kyligence.kap.smart.model.proposer;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.smart.model.ModelContext;
import io.kyligence.kap.smart.model.ModelContextBuilder;
import io.kyligence.kap.smart.query.Utils;

public class SimpleScopeProposerTest {
    private static KylinConfig kylinConfig = Utils.newKylinConfig("src/test/resources/learn_kylin/meta");

    @BeforeClass
    public static void beforeClass() {
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.AdHocRunnerSparkImpl");
    }

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testOnStarModel() throws JsonProcessingException {
        final String project = "learn_kylin";
        ModelContextBuilder contextBuilder = new ModelContextBuilder(kylinConfig, project);
        List<ModelContext> context = contextBuilder.buildFromSQLs(
                new String[] { "select lstg_format_name, sum(price) from kylin_sales group by lstg_format_name" });

        KapModel modelDesc = (KapModel) DataModelManager.getInstance(kylinConfig)
                .getDataModelDesc("kylin_sales_model_star");
        KapModel newModelDesc = KapModel.getCopyOf(modelDesc);
        newModelDesc.getDimensions().clear();

        SimpleScopeProposer proposer = new SimpleScopeProposer(context.get(0));
        newModelDesc = proposer.propose(newModelDesc);
        newModelDesc.init(kylinConfig, TableMetadataManager.getInstance(kylinConfig).getAllTablesMap(project),
                Lists.<DataModelDesc> newArrayList(), false);

        Assert.assertEquals(modelDesc.getJoinTables().length + 1, newModelDesc.getDimensions().size());
        Assert.assertEquals(modelDesc.getRootFactTable().getColumns().size(),
                newModelDesc.getDimensions().get(0).getColumns().length);
    }
}
