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

import com.sun.tools.javac.util.Assert;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class NAggPushDownTest extends NLocalWithSparkSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(NAggPushDownTest.class);
    private String sqlFolder = "sql_select_subquery";
    private String joinType = "inner"; // only support inner join

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.query.match-partial-inner-join-model", "true");
        System.setProperty("kylin.query.calcite.aggregate-pushdown-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/agg_push_down");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.query.match-partial-inner-join-model");
        System.clearProperty("kylin.query.calcite.aggregate-pushdown-enabled");
    }

    @Override
    public String getProject() {
        return "subquery";
    }

    @Test
    public void testPushDown() throws Exception {
        fullBuildCube("a749e414-c40e-45b7-92e4-bbfe63af705d", getProject());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecAndComp.CompareLevel compareLevel = NExecAndComp.CompareLevel.SAME;
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        String identity = "sqlFolder:" + sqlFolder + ", joinType:" + joinType + ", compareLevel:" + compareLevel;
        try {
            List<Pair<String, String>> queries = NExecAndComp
                    .fetchQueries(KAP_SQL_BASE_DIR + File.separator + sqlFolder);
            NExecAndComp.execAndCompare(queries, getProject(), compareLevel, joinType);
        } catch (Throwable th) {
            logger.error("Query fail on:", identity);
            Assert.error();
        }
        logger.info("Query succeed on:", identity);
    }
}