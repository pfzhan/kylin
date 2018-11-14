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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.KylinConfigUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.spark.KapSparkSession;

public class NAutoTestBase extends NLocalWithSparkSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(NAutoTestBase.class);
    protected static KylinConfig kylinConfig;
    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/query";

    @Before
    public void setup() throws Exception {
        super.init();
        kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.storage.provider.0", "io.kyligence.kap.storage.NDataStorage");
        kylinConfig.setProperty("kap.storage.columnar.hdfs-dir", kylinConfig.getHdfsWorkingDirectory() + "/parquet/");
        kylinConfig.setProperty("kap.smart.conf.model.inner-join.exactly-match", "true");
        KylinConfigUtils.setH2DriverAsFavoriteQueryStorageDB(kylinConfig);
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        ResourceStore.clearCache(kylinConfig);
        System.clearProperty("kylin.job.scheduler.poll-interval-second");

        Candidate.restorePriorities();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    class TestScenario {

        String name;
        List<Pair<String, String>> queries;
        CompareLevel compareLevel;
        String joinType;

        public TestScenario(String name, CompareLevel compareLevel) throws Exception {
            this(name, compareLevel, 0, 0, "default", new String[0]);
        }

        public TestScenario(String name, CompareLevel compareLevel, int start, int end) throws Exception {
            this(name, compareLevel, start, end, "default", new String[0]);
        }

        public TestScenario(String name, CompareLevel compareLevel, String[] exclusionList) throws Exception {
            this(name, compareLevel, 0, 0, "default", exclusionList);
        }

        public TestScenario(String name, CompareLevel compareLevel, String joinType) throws Exception {
            this(name, compareLevel, 0, 0, joinType, new String[0]);
        }

        public TestScenario(String name, CompareLevel compareLevel, int start, int end, String joinType,
                String[] exclusionList) throws Exception {
            this.name = name;
            this.compareLevel = compareLevel;
            this.joinType = joinType;
            this.queries = fetchPartialQueries(name, start, end, joinType, exclusionList);
        }

        public void execute() throws Exception {
            executeTestScenario(this);
        }
    }

    @SuppressWarnings("unchecked")
    protected void executeTestScenario(TestScenario... tests) throws Exception {

        List<Pair<String, String>> queries = new ArrayList<>();
        for (TestScenario test : tests) {
            queries.addAll(test.queries);
        }

        buildCubeWithSparkSession(queries);

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(getProject());

        // Validate results between SparkSQL and cube
        populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);

        for (TestScenario test : tests) {
            try {
                NExecAndComp.execAndCompare(test.queries, kapSparkSession, test.compareLevel, test.joinType);
            } catch (Exception e) {
                logger.error("'{}' failed.", test.name);
                throw e;
            }
        }

        kapSparkSession.close();
    }

    protected NSmartMaster proposeCubeWithSmartMaster(List<Pair<String, String>> queries) throws Exception {
        List<String> sqlList = new ArrayList<>();
        for (Pair<String, String> queryPair : queries) {
            sqlList.add(QueryPatternUtil.normalizeSQLPattern(queryPair.getSecond()));
        }

        NSmartMaster master = new NSmartMaster(kylinConfig, getProject(), sqlList.toArray(new String[0]));
        master.runAll();
        return master;
    }

    protected void buildCubeWithSparkSession(List<Pair<String, String>> queries) throws Exception {
        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        proposeCubeWithSmartMaster(queries);
        kapSparkSession.use(getProject());
        kapSparkSession.buildAllCubes(kylinConfig, getProject());
        kapSparkSession.close();
    }

    protected List<Pair<String, String>> fetchPartialQueries(String subFolder, int start, int end, String joinType)
            throws IOException {

        return fetchPartialQueries(subFolder, start, end, joinType, new String[0]);
    }

    protected List<Pair<String, String>> fetchPartialQueries(String subFolder, int start, int end, String joinType,
            String[] exclusionList) throws IOException {
        String folder = IT_SQL_KAP_DIR + File.separator + subFolder;
        List<Pair<String, String>> partials = start < end ? NExecAndComp.fetchPartialQueries(folder, start, end)
                : NExecAndComp.fetchQueries(folder);
        for (Pair<String, String> pair : partials) {
            String sql = pair.getSecond();
            String transformedQuery = QueryUtil.massageSql(sql, getProject(), 0, 0, "DEFAULT");
            pair.setSecond(NExecAndComp.changeJoinType(transformedQuery, joinType));
        }
        return NExecAndComp.doFilter(partials, exclusionList);
    }
}
