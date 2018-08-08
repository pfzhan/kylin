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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.spark.KapSparkSession;

public class NMatchingTest extends NLocalWithSparkSessionTest {

    private static final String DEFAULT_PROJECT = "match";

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testCanNotAnswer() throws Exception {
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        ss.sparkContext().setLogLevel("ERROR");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        fullBuildCube("match", DEFAULT_PROJECT);
        ss.close();

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);

        try {
            populateSSWithCSVData(config, DEFAULT_PROJECT, kapSparkSession);

            List<Pair<String, String>> query = new ArrayList<>();
            query.add(
                    Pair.newPair("can_not_answer", "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
            NExecAndComp.execAndCompare(query, kapSparkSession, CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getCause().getMessage().contains("No realization found for OLAPContext"));
        }

    }

    @Test
    public void testCanAnswer() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        fullBuildCube("match", DEFAULT_PROJECT);
        fullBuildCube("match_copy", DEFAULT_PROJECT);
        ss.close();

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);

        populateSSWithCSVData(config, DEFAULT_PROJECT, kapSparkSession);

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_not_answer", "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        NExecAndComp.execAndCompare(query, kapSparkSession, CompareLevel.SAME, "left");
    }
}
