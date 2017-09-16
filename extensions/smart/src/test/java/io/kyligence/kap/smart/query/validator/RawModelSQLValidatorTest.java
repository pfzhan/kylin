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

package io.kyligence.kap.smart.query.validator;

import java.util.Arrays;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.model.ModelMasterTest;

public class RawModelSQLValidatorTest extends ModelMasterTest {

    private static final Logger logger = LoggerFactory.getLogger(RawModelSQLValidator.class);

    @Ignore
    @Test
    public void testE2E_LearnKylin() throws Exception {
        Map<String, SQLValidateResult> results = testInternal("src/test/resources/learn_kylin/meta", "learn_kylin", "kylin_sales",
                "src/test/resources/learn_kylin/sql");
        Assert.assertEquals(1, results.size());
        for (SQLValidateResult result : results.values()) {
            Assert.assertTrue(result.isCapable());
        }
    }

    @Ignore
    @Test
    public void testE2E_TPCDS_ss() throws Exception {
        Map<String, SQLValidateResult> results = testInternal("src/test/resources/tpcds/meta", "TPC_DS_2",
                "TPCDS_BIN_PARTITIONED_ORC_2.STORE_SALES", "src/test/resources/tpcds/sql_ss");
        Assert.assertEquals(3, results.size());
        for (SQLValidateResult result : results.values()) {
            Assert.assertTrue(result.isCapable());
        }
    }

    @Ignore
    @Test
    public void testE2E_TPCDS_badQuery() throws Exception {
        Map<String, SQLValidateResult> results = testInternal("src/test/resources/tpcds/meta", "TPC_DS_2",
                "TPCDS_BIN_PARTITIONED_ORC_2.STORE_SALES", "src/test/resources/tpcds/sql_badquery");
        Assert.assertEquals(1, results.size());
        for (SQLValidateResult result : results.values()) {
            Assert.assertFalse(result.isCapable());
        }
    }

    private Map<String, SQLValidateResult> testInternal(String metaDir, String project, String factTable, String sqlDir) throws Exception {
        KylinConfig kylinConfig = prepareConfig(metaDir);
        String[] sqls = loadQueries(sqlDir);

        logger.info("Test auto modeling with fact table {}.", factTable);

        RawModelSQLValidator validator = new RawModelSQLValidator(kylinConfig, project, factTable);
        Map<String, SQLValidateResult> results = validator.batchValidate(Arrays.asList(sqls));
        printResult(results);
        
        try {
        ModelMaster master = validator.buildValidatedModelMaster();
        DataModelDesc modelDesc = getValidatedModel(master, kylinConfig, project);
        System.out.println(JsonUtil.writeValueAsIndentString(modelDesc));
        } catch (IllegalStateException e) {
            // Skip IllegalStateException of buildValidatedModelMaster()
        }

        return results;
    }
    
    private void printResult(Map<String, SQLValidateResult> results) {
        for (Map.Entry<String, SQLValidateResult> result : results.entrySet()) {
            System.out.println("==========================================");
            System.out.println(result.getKey());
            System.out.println("------------------------------------------");
            System.out.println(result.getValue());
            System.out.println("==========================================\n");
        }
    }
    
    private DataModelDesc getValidatedModel(ModelMaster master, KylinConfig kylinConfig, String project) throws Exception {
        DataModelDesc modelDesc = master.proposeAll();
        modelDesc.init(kylinConfig, MetadataManager.getInstance(kylinConfig).getAllTablesMap(project),
                Lists.<DataModelDesc> newArrayList());
        return modelDesc;
    }
}
