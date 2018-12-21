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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.spark.KapSparkSession;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.AccelerationMatchedLevel;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.CompareEntity;

public class NAutoTestBase extends NLocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NAutoTestBase.class);

    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/query";
    private Map<String, String> systemProp = Maps.newHashMap();
    protected KylinConfig kylinConfig;

    @Before
    public void setup() throws Exception {
        super.init();
        kylinConfig = getTestConfig();
        overwriteSystemProp("kap.smart.conf.model.inner-join.exactly-match", "true");
    }

    void overwriteSystemProp(String key, String value) {
        systemProp.put(key, System.getProperty(key));
        System.setProperty(key, value);
    }

    private void restoreAllSystemProp() {
        systemProp.forEach((prop, value) -> {
            if (value == null) {
                logger.info("CLear {}", prop);
                System.clearProperty(prop);
            } else {
                logger.info("restore {}", prop);
                System.setProperty(prop, value);
            }
        });
        systemProp.clear();
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        ResourceStore.clearCache(kylinConfig);
        System.clearProperty("kylin.job.scheduler.poll-interval-second");

        Candidate.restorePriorities();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        restoreAllSystemProp();
    }

    String getFolder(String subFolder) {
        return IT_SQL_KAP_DIR + File.separator + subFolder;
    }

    protected void executeTestScenario(TestScenario... testScenarios) throws Exception {

        // 1. execute auto-modeling propose
        List<String> sqlList = Lists.newArrayList();
        for (TestScenario testScenario : testScenarios) {
            testScenario.collectQueries();
            final List<String> tmpSqls = testScenario.queries.stream().map(Pair::getSecond)
                    .collect(Collectors.toList());
            sqlList.addAll(tmpSqls);
        }

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));

        String[] sqls = sqlList.toArray(new String[0]);
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        final Map<String, CompareEntity> compareMap = collectCompareEntity(smartMaster);
        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            // 2. execute cube building
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            // 3. validate results between SparkSQL and cube
            Arrays.stream(testScenarios).forEach(testScenario -> {
                populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);
                if (testScenario.isLimit) {
                    NExecAndComp.execLimitAndValidateNew(testScenario.queries, kapSparkSession, JoinType.DEFAULT.name(),
                            compareMap);
                } else {
                    NExecAndComp.execAndCompareNew(testScenario.queries, kapSparkSession, testScenario.compareLevel,
                            testScenario.joinType.name(), compareMap);
                }

                // 4. compare layout propose result and query cube result
                RecAndQueryCompareUtil.computeCompareRank(kylinConfig, getProject(), compareMap);
                compareMap.forEach((key, value) -> System.out.println(value.toString() + '\n'));
                // TODO use assert in the future #9318
            });
        }

        // 5. summary info
        final Map<AccelerationMatchedLevel, AtomicInteger> rankInfoMap = RecAndQueryCompareUtil
                .summarizeRankInfo(compareMap);
        StringBuilder sb = new StringBuilder();
        sb.append("All used queries: ").append(compareMap.size()).append('\n');
        rankInfoMap.forEach((key, value) -> sb.append(key).append(": ").append(value).append("\n"));
        System.out.println(sb);
    }

    List<Pair<String, String>> fetchQueries(String subFolder, int fromIndex, int toIndex) throws IOException {
        List<Pair<String, String>> queries;
        String folder = getFolder(subFolder);
        if (fromIndex == toIndex) {
            queries = NExecAndComp.fetchQueries(folder);
        } else {
            if (fromIndex > toIndex) {
                int tmp = fromIndex;
                fromIndex = toIndex;
                toIndex = tmp;
            }
            queries = NExecAndComp.fetchPartialQueries(folder, fromIndex, toIndex);
        }
        return queries;

    }

    public class TestScenario {

        String folderName;
        CompareLevel compareLevel;
        JoinType joinType;
        private int fromIndex;
        private int toIndex;
        private boolean isLimit;
        private Set<String> exclusionList;

        // value when execute
        private List<Pair<String, String>> queries;

        TestScenario(CompareLevel compareLevel, String folder) {
            this(compareLevel, JoinType.DEFAULT, folder);
        }

        TestScenario(CompareLevel compareLevel, JoinType joinType, String folder) {
            this(compareLevel, joinType, false, folder, 0, 0, null);
        }

        public TestScenario(CompareLevel compareLevel, String folder, int fromIndex, int toIndex) {
            this(compareLevel, JoinType.DEFAULT, false, folder, fromIndex, toIndex, null);
        }

        TestScenario(CompareLevel compareLevel, String folder, Set<String> exclusionList) {
            this(compareLevel, JoinType.DEFAULT, false, folder, 0, 0, exclusionList);
        }

        TestScenario(CompareLevel compareLevel, boolean isLimit, String folder) {
            this(compareLevel, JoinType.DEFAULT, isLimit, folder, 0, 0, null);
        }

        private TestScenario(CompareLevel compareLevel, JoinType joinType, boolean isLimit, String folderName,
                int fromIndex, int toIndex, Set<String> exclusionList) {
            this.compareLevel = compareLevel;
            this.folderName = folderName;
            this.joinType = joinType;
            this.isLimit = isLimit;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.exclusionList = exclusionList;
        }

        public void execute() throws Exception {
            executeTestScenario(this);
        }

        void collectQueries() throws IOException {
            queries = fetchQueries(folderName, fromIndex, toIndex);
            normalizeSql(joinType, queries);
            queries = exclusionList == null ? queries : NExecAndComp.doFilter(queries, exclusionList);
        }

        private void normalizeSql(JoinType joinType, List<Pair<String, String>> queries) {
            queries.forEach(pair -> {
                String transformedQuery = QueryUtil.massageSql(pair.getSecond(), getProject(), 0, 0, "DEFAULT");
                transformedQuery = QueryUtil.removeCommentInSql(transformedQuery);
                String tmp = KylinTestBase.changeJoinType(transformedQuery, joinType.name());
                // tmp = QueryPatternUtil.normalizeSQLPattern(tmp); // something wrong with sqlPattern, skip this step
                pair.setSecond(tmp);
            });
        }

    } // end TestScenario

    Map<String, CompareEntity> collectCompareEntity(NSmartMaster smartMaster) {
        Map<String, CompareEntity> map = Maps.newHashMap();
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((sql, accelerateInfo) -> {
            map.putIfAbsent(sql, new CompareEntity());
            final CompareEntity entity = map.get(sql);
            entity.setAccelerateInfo(accelerateInfo);
            entity.setAccelerateLayouts(RecAndQueryCompareUtil.writeQueryLayoutRelationAsString(kylinConfig,
                    getProject(), accelerateInfo.getRelatedLayouts()));
            entity.setSql(sql);
        });
        return map;
    }

    public enum JoinType {

        /**
         * Left outer join.
         */
        LEFT,

        /**
         * Inner join
         */
        INNER,

        /**
         * original state
         */
        DEFAULT
    }
}
