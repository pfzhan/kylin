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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.KylinTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.common.SparderQueryTest;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NEscapedTest extends NAutoTestBase {
    @Test
    public void testSimilarTo() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        executeTestScenario(2, new TestScenario(NExecAndComp.CompareLevel.SAME, "query/sql_escaped"));
    }

    protected void buildAndCompare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            TestScenario... testScenarios) throws Exception {
        try {
            // 2. execute cube building
            long startTime = System.currentTimeMillis();
            buildAllCubes(kylinConfig, getProject());
            log.debug("build cube cost {} ms", System.currentTimeMillis() - startTime);

            // dump metadata for debugging
            dumpMetadata();

            // 3. validate results between SparkSQL and cube
            startTime = System.currentTimeMillis();
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            Arrays.stream(testScenarios).forEach(testScenario -> {
                populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
                execAndCompareEscaped(testScenario.getQueries(), getProject(), testScenario.getCompareLevel(),
                        testScenario.getJoinType().name(), compareMap);
            });
            log.debug("compare result cost {} s", System.currentTimeMillis() - startTime);
        } finally {
            FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        }
    }

    public void execAndCompareEscaped(List<Pair<String, String>> queries, String prj,
            NExecAndComp.CompareLevel compareLevel, String joinType,
            Map<String, RecAndQueryCompareUtil.CompareEntity> recAndQueryResult) {
        for (Pair<String, String> query : queries) {
            log.info("Exec and compare query ({}) :{}", joinType, query.getFirst());
            String sql = KylinTestBase.changeJoinType(query.getSecond(), joinType);

            // query not escaped sql from cube and spark
            long startTime = System.currentTimeMillis();
            Dataset<Row> cubeResult = NExecAndComp.queryWithKap(prj, joinType, Pair.newPair(sql, sql),
                    recAndQueryResult);
            NExecAndComp.addQueryPath(recAndQueryResult, query, sql);
            Dataset<Row> sparkResult = NExecAndComp.queryWithSpark(prj, sql, query.getFirst());

            // make ke not escape sql and escape sql manually
            overwriteSystemProp("kylin.query.parser.escaped-string-literals", "true");
            val eSql = sql.replace("\\\\", "\\");

            // query escaped sql from cube and spark
            Dataset<Row> cubeResult2 = NExecAndComp.queryWithKap(prj, joinType, Pair.newPair(eSql, eSql),
                    Maps.newHashMap());
            SparderEnv.getSparkSession().conf().set("spark.sql.parser.escapedStringLiterals", true);
            Dataset<Row> sparkResult2 = NExecAndComp.queryWithSpark(prj, eSql, query.getFirst());
            if ((compareLevel == NExecAndComp.CompareLevel.SAME || compareLevel == NExecAndComp.CompareLevel.SAME_ORDER)
                    && sparkResult.schema().fields().length != cubeResult.schema().fields().length) {
                log.error("Failed on compare query ({}) :{} \n cube schema: {} \n, spark schema: {}", joinType, query,
                        cubeResult.schema().fieldNames(), sparkResult.schema().fieldNames());
                throw new IllegalStateException("query (" + joinType + ") :" + query + " schema not match");
            }

            List<Row> sparkRows = sparkResult.collectAsList();
            List<Row> kapRows = SparderQueryTest.castDataType(cubeResult, sparkResult).collectAsList();

            List<Row> sparkRows2 = sparkResult2.collectAsList();
            List<Row> kapRows2 = SparderQueryTest.castDataType(cubeResult2, sparkResult).collectAsList();

            // compare all result set
            if (!NExecAndComp.compareResults(NExecAndComp.normRows(sparkRows), NExecAndComp.normRows(kapRows),
                    compareLevel)
                    || !NExecAndComp.compareResults(NExecAndComp.normRows(sparkRows), NExecAndComp.normRows(sparkRows2),
                            compareLevel)
                    || !NExecAndComp.compareResults(NExecAndComp.normRows(kapRows), NExecAndComp.normRows(kapRows2),
                            compareLevel)) {
                log.error("Failed on compare query ({}) :{}", joinType, query);
                throw new IllegalArgumentException("query (" + joinType + ") :" + query + " result not match");
            }
            log.info("The query ({}) : {} cost {} (ms)", joinType, query, System.currentTimeMillis() - startTime);

            // restore env
            restoreSystemProp("kylin.query.parser.escaped-string-literals");
            SparderEnv.getSparkSession().conf().unset("spark.sql.parser.escapedStringLiterals");
        }
    }
}
