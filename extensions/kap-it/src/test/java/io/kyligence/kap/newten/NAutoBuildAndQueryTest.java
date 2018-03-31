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

import io.kyligence.kap.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.spark.KapSparkSession;

public class NAutoBuildAndQueryTest extends NLocalWithSparkSessionTest {

    private KylinConfig kylinConfig;
    private KapSparkSession kapSparkSession;
    private static final String NEWTEN_PROJECT = "newten";

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        super.setUp();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.storage.provider.0", "io.kyligence.kap.storage.NDataStorage");
        kylinConfig.setProperty("kap.storage.columnar.hdfs-dir", kylinConfig.getHdfsWorkingDirectory() + "/parquet/");
    }

    @After
    public void after() throws Exception {
        Candidate.restorePriorities();

        if (kapSparkSession != null)
            kapSparkSession.close();

        NDefaultScheduler.destroyInstance();
        super.tearDown();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    /**
     *
     * This test has only finished partial IT queries, because auto modeling can not handle all the queries yet.
     * it should be capable to process all the IT queries.
     *
     * */

    @Test
    public void runITQueries() throws Exception {

        // Step1. Auto modeling and cubing
        //String[] queries = retrieveAllQueries(KYLIN_SQL_BASE_DIR);
        List<Pair<String, String>> queries = NExecAndComp
                .fetchPartialQueries(KYLIN_SQL_BASE_DIR + File.separator + "sql", 0, 3);
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(NEWTEN_PROJECT);
        for (Pair<String, String> query : queries) {
            kapSparkSession.collectQueries(query.getSecond());
        }
        kapSparkSession.speedUp();

        // Step2. Query cube and query SparkSQL respectively
        kapSparkSession.close();
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(NEWTEN_PROJECT);

        // Step3. Validate results between sparksql and cube

        populateSSWithCSVData(kylinConfig, NEWTEN_PROJECT, kapSparkSession);
        NExecAndComp.execAndCompare(queries, kapSparkSession, NExecAndComp.CompareLevel.SAME, "left");
    }

}
