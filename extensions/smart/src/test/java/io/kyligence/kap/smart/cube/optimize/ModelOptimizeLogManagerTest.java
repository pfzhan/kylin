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

package io.kyligence.kap.smart.cube.optimize;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.query.util.CognosParenthesesEscape;
import io.kyligence.kap.smart.cube.ModelOptimizeLog;
import io.kyligence.kap.smart.cube.ModelOptimizeLogManager;
import io.kyligence.kap.smart.query.Utils;
import io.kyligence.kap.smart.query.validator.RawModelSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

public class ModelOptimizeLogManagerTest extends LocalFileMetadataTestCase {

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Ignore
    @Test
    public void testBasics() throws IOException {
        String modelName = "test_model";
        String[] sqls = { "select * from table1", "select count(*) from table2", "select sum(column1) from table3",
                "select lstg_format_name, sum(price) from kylin_sales group by lstg_format_name" };

        KylinConfig kylinConfig = Utils.newKylinConfig("src/test/resources/smart/learn_kylin/meta");
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.AdHocRunnerSparkImpl");
        kylinConfig.setProperty("kap.smart.conf.model.scope.strategy", "query");
        kylinConfig.setProperty("kylin.query.transformers", CognosParenthesesEscape.class.getName());
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        ModelOptimizeLogManager manager = ModelOptimizeLogManager.getInstance(kylinConfig);
        manager.removeModelOptimizeLog(modelName);

        RawModelSQLValidator validator = new RawModelSQLValidator(kylinConfig, "learn_kylin", "kylin_sales");
        Map<String, SQLValidateResult> results = validator.batchValidate(Arrays.asList(sqls));
        List<SQLValidateResult> orderedResult = new ArrayList<>(sqls.length);
        for (String sql : sqls) {
            orderedResult.add(results.get(sql));
        }

        ModelOptimizeLog modelOptimizeLog = manager.getModelOptimizeLog(modelName);
        if (modelOptimizeLog == null) {
            modelOptimizeLog = new ModelOptimizeLog();
            modelOptimizeLog.setModelName(modelName);
        }
        modelOptimizeLog.setSampleSqls(Arrays.asList(sqls));
        modelOptimizeLog.setSqlValidateResult(orderedResult);
        manager.saveModelOptimizeLog(modelOptimizeLog);

        ModelOptimizeLog newOne = manager.getModelOptimizeLog(modelName);
        List<String> sampleSqls = newOne.getSampleSqls();
        assertEquals(4, sampleSqls.size());
        
        manager.removeModelOptimizeLog(modelName);
        ModelOptimizeLog deletedOne = manager.getModelOptimizeLog(modelName);
        Assert.assertNull(deletedOne);
    }
}
