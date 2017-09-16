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
package io.kyligence.kap.query.mockup;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.validator.AbstractSQLValidator;
import io.kyligence.kap.smart.query.validator.CubeSQLValidator;
import io.kyligence.kap.smart.query.validator.ModelSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

/**
 * Created by shengping.liao on 07/09/2017.
 */
public class SqlValidatorTest extends LocalFileMetadataTestCase {
    private KylinConfig config;
    private String[] sqls;

    @After
    public void cleanup() throws IOException {
        cleanAfterClass();
    }

    @Before
    public void setup() throws IOException {
        createTestMetadata();
        config = KylinConfig.getInstanceFromEnv();
//        sqls = new String[]{"select count(*) from test_kylin_fact", "select 1", "abcd", "select * from WIDE_TABLE",
//                "select a,b,c from test_kylin_fact", "select price from test_kylin_fact group by price",
//        "SELECT \n" +
//                " test_cal_dt.week_beg_dt \n" +
//                " ,count(distinct test_category_groupings.BSNS_VRTCL_NAME)\n" +
//                " ,test_category_groupings.meta_categ_name \n" +
//                " ,test_category_groupings.categ_lvl2_name \n" +
//                " ,sum(price) as GMV, count(*) as TRANS_CNT \n" +
//                " FROM test_kylin_fact \n" +
//                " inner JOIN test_category_groupings \n" +
//                " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id \n" +
//                " AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id \n" +
//                " inner JOIN edw.test_cal_dt as test_cal_dt \n" +
//                " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt \n" +
//                " group by test_cal_dt.week_beg_dt \n" +
//                " ,test_category_groupings.meta_categ_name \n" +
//                " ,test_category_groupings.categ_lvl2_name","select * from test_kylin_fact inner join STREAMING_TABLE ON test_kylin_fact.CAL_DT = STREAMING_TABLE.day_start"};
        sqls = new String[]{"set a=1"};
    }

    @Test
    @Ignore
    public void testModel() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc("ci_inner_join_cube");
        AbstractSQLValidator sqlValidator = new ModelSQLValidator(config, cubeDesc.getModel());
        Map<String, SQLValidateResult> validateStatsMap = sqlValidator.batchValidate(Lists.newArrayList(sqls));
        print(validateStatsMap);
    }

    @Test
    @Ignore
    public void testCube() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc("ci_inner_join_cube");
        AbstractSQLValidator sqlValidator = new CubeSQLValidator(config, cubeDesc);
        Map<String, SQLValidateResult> validateStatsMap = sqlValidator.batchValidate(Lists.newArrayList(sqls));
        print(validateStatsMap);
    }

    private void print(Map<String, SQLValidateResult> validateStatsMap) {
        for (Map.Entry<String, SQLValidateResult> entry : validateStatsMap.entrySet()) {
            String message = "";
            for (SQLAdvice sqlAdvice : entry.getValue().getSQLAdvices()) {
                message += String.format("reason:%s, \n\t  suggest:%s \n\t", sqlAdvice.getIncapableReason(), sqlAdvice.getSuggestion());
            }
            System.out.println(String.format("sql:%s, \n\t  capable:%s, \n\t  %s", entry.getKey(), entry.getValue().isCapable(), message));
        }
    }
}
