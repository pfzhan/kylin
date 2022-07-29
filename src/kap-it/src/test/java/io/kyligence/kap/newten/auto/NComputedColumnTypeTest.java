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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.engine.spark.utils.ComputedColumnEvalUtil;

public class NComputedColumnTypeTest extends NLocalWithSparkSessionTest {

    @Test
    public void testDataTypeForNestedCC() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String project = getProject();
        populateSSWithCSVData(config, project, SparderEnv.getSparkSession());
        
        NDataModelManager manager = NDataModelManager.getInstance(config, project);
        NDataModel model = manager.getDataModelDescByAlias("nmodel_basic");
        
        ComputedColumnDesc newCC = new ComputedColumnDesc();
        newCC.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        newCC.setTableAlias("TEST_KYLIN_FACT");
        newCC.setColumnName("NEW_CC");
        newCC.setExpression("TEST_KYLIN_FACT.NEST4 - 1");
        newCC.setInnerExpression(KapQueryUtil.massageComputedColumn(model, project, newCC, null));
        newCC.setDatatype("ANY");
        
        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(model, Lists.newArrayList(newCC));
        Assert.assertEquals("DECIMAL(32,0)", newCC.getDatatype());
    }
    
    @Test
    public void testAllDataTypesForCC() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String project = getProject();
        populateSSWithCSVData(config, project, SparderEnv.getSparkSession());
        
        NDataModelManager manager = NDataModelManager.getInstance(config, project);
        NDataModel model = manager.getDataModelDescByAlias("nmodel_full_measure_test");

        Map<String, String> exprTypes = Maps.newHashMap();
        exprTypes.put("TEST_MEASURE.ID1", "BIGINT");
        exprTypes.put("TEST_MEASURE.ID2", "BIGINT");
        exprTypes.put("TEST_MEASURE.ID3", "BIGINT"); // long
        exprTypes.put("TEST_MEASURE.ID4", "INTEGER");
        exprTypes.put("TEST_MEASURE.ID1 * 2", "BIGINT");
        exprTypes.put("TEST_MEASURE.ID2 / 2", "DOUBLE");
        exprTypes.put("TEST_MEASURE.ID3 - 1000", "BIGINT");
        exprTypes.put("TEST_MEASURE.ID4 + 1000", "INTEGER");
        exprTypes.put("CASE WHEN TEST_MEASURE.ID1 > 0 THEN 'YES' ELSE 'NO' END", "VARCHAR");

        exprTypes.put("TEST_MEASURE.PRICE1", "FLOAT");
        exprTypes.put("TEST_MEASURE.PRICE2", "DOUBLE");
        exprTypes.put("TEST_MEASURE.PRICE3", "DECIMAL(19,4)");
        exprTypes.put("TEST_MEASURE.PRICE5", "SMALLINT"); // short
        exprTypes.put("TEST_MEASURE.PRICE6", "TINYINT");
        exprTypes.put("TEST_MEASURE.PRICE7", "SMALLINT");
        exprTypes.put("TEST_MEASURE.PRICE1 - TEST_MEASURE.PRICE2", "DOUBLE");
        exprTypes.put("CASE WHEN 1 > 0 THEN TEST_MEASURE.PRICE1 ELSE TEST_MEASURE.PRICE2 END", "DOUBLE");
        exprTypes.put("CASE WHEN 1 < 0 THEN TEST_MEASURE.PRICE1 ELSE TEST_MEASURE.PRICE2 END", "DOUBLE");
        exprTypes.put("TEST_MEASURE.PRICE3 * 10", "DECIMAL(22,4)");
        exprTypes.put("TEST_MEASURE.PRICE3 / 10", "DECIMAL(22,7)");
        exprTypes.put("TEST_MEASURE.PRICE5 + 1", "INTEGER");
        exprTypes.put("TEST_MEASURE.PRICE6 + 1", "INTEGER");
        exprTypes.put("TEST_MEASURE.PRICE7 + 1", "INTEGER");
        exprTypes.put("TEST_MEASURE.PRICE5 * TEST_MEASURE.PRICE6 + TEST_MEASURE.PRICE7", "SMALLINT");

        exprTypes.put("TEST_MEASURE.NAME1", "VARCHAR"); // string
        exprTypes.put("TEST_MEASURE.NAME2", "VARCHAR"); // varchar(254)
        exprTypes.put("TEST_MEASURE.NAME3", "VARCHAR"); // char
        exprTypes.put("TEST_MEASURE.NAME4", "TINYINT"); // byte
        exprTypes.put("CONCAT(TEST_MEASURE.NAME1, ' ')", "VARCHAR");
        exprTypes.put("SUBSTRING(TEST_MEASURE.NAME2, 1, 2)", "VARCHAR");
        exprTypes.put("LENGTH(TEST_MEASURE.NAME1)", "INTEGER");
        
        exprTypes.put("TEST_MEASURE.TIME1", "DATE");
        exprTypes.put("TEST_MEASURE.TIME2", "TIMESTAMP");
        exprTypes.put("DATEDIFF(CAST(TEST_MEASURE.TIME2 AS DATE), TEST_MEASURE.TIME1)", "INTEGER");
        exprTypes.put("CAST(TEST_MEASURE.TIME2 AS STRING)", "VARCHAR");
        exprTypes.put("TEST_MEASURE.TIME1 + INTERVAL 12 HOURS", "TIMESTAMP");
        exprTypes.put("TEST_MEASURE.TIME2 + INTERVAL 12 HOURS", "TIMESTAMP");
        exprTypes.put("YEAR(TEST_MEASURE.TIME2)", "INTEGER");
        exprTypes.put("MONTH(TEST_MEASURE.TIME2)", "INTEGER");
        exprTypes.put("DAYOFMONTH(TEST_MEASURE.TIME2)", "INTEGER");

        exprTypes.put("TEST_MEASURE.FLAG", "BOOLEAN");
        exprTypes.put("NOT TEST_MEASURE.FLAG", "BOOLEAN");

        AtomicInteger ccId = new AtomicInteger(0);
        List<ComputedColumnDesc> newCCs = exprTypes.keySet().stream().map(expr -> {
            ComputedColumnDesc newCC = new ComputedColumnDesc();
            newCC.setTableIdentity("DEFAULT.TEST_MEASURE");
            newCC.setTableAlias("TEST_MEASURE");
            newCC.setColumnName("CC_" + ccId.incrementAndGet());
            newCC.setExpression(expr);
            newCC.setDatatype("ANY");
            return newCC;
        }).collect(Collectors.toList());

        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(model, newCCs);
        newCCs.stream().forEach(cc -> {
            String expr = cc.getExpression();
            Assert.assertEquals(expr + " type is fail", exprTypes.get(expr), cc.getDatatype());
        });
    }
}
