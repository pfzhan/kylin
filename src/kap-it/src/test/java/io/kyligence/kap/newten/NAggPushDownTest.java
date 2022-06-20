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

package io.kyligence.kap.newten;

import java.io.File;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.tools.javac.util.Assert;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.util.ExecAndComp;

public class NAggPushDownTest extends NLocalWithSparkSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(NAggPushDownTest.class);
    private String sqlFolder = "sql_select_subquery";
    private String joinType = "inner"; // only support inner join

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        overwriteSystemProp("kylin.query.calcite.aggregate-pushdown-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/agg_push_down");
        //TODO need to be rewritten
        //        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        //        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        //        if (!scheduler.hasStarted()) {
        //            throw new RuntimeException("scheduler has not been started");
        //        }
    }

    @After
    public void after() throws Exception {
        //TODO need to be rewritten
        // NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "subquery";
    }

    @Test
    public void testBasic() throws Exception {
        fullBuild("a749e414-c40e-45b7-92e4-bbfe63af705d");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ExecAndComp.CompareLevel compareLevel = ExecAndComp.CompareLevel.SAME;
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String identity = "sqlFolder:" + sqlFolder + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KAP_SQL_BASE_DIR + File.separator + sqlFolder);
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
            Assert.error();
        }
        logger.info("Query succeed on: {}", identity);
    }

    @Test
    public void testAggPushDown() throws Exception {
        fullBuild("ce2057da-54c8-4e05-b0bf-d225a6bbb62c");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ExecAndComp.CompareLevel compareLevel = ExecAndComp.CompareLevel.SAME;
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String identity = "sqlFolder:" + "sql_agg_pushdown" + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
        try {
            List<Pair<String, String>> queries = ExecAndComp
                    .fetchQueries(KAP_SQL_BASE_DIR + File.separator + "sql_agg_pushdown");
            ExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
        } catch (Throwable th) {
            logger.error("Query fail on: {}", identity);
            Assert.error();
        }
        logger.info("Query succeed on: {}", identity);
    }
}
