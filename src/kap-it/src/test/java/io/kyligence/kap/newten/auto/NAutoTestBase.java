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
import java.io.FileInputStream;
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
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.spark.KapSparkSession;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.AccelerationMatchedLevel;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.CompareEntity;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NAutoTestBase extends NLocalWithSparkSessionTest {

    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/query";
    private Map<String, String> systemProp = Maps.newHashMap();
    protected KylinConfig kylinConfig;
    private static Set<String> excludedSqlPatterns;
    private static final String FILE_SEPARATOR = System.getProperty("line.separator");

    @Before
    public void setup() throws Exception {
        super.init();
        kylinConfig = getTestConfig();
        if (excludedSqlPatterns == null) {
            excludedSqlPatterns = loadWhiteListSqlPatterns();
        }
    }

    @Override
    public String getProject() {
        return "newten";
    }

    void overwriteSystemProp(String key, String value) {
        systemProp.put(key, System.getProperty(key));
        System.setProperty(key, value);
    }

    private void restoreAllSystemProp() {
        systemProp.forEach((prop, value) -> {
            if (value == null) {
                log.info("Clear {}", prop);
                System.clearProperty(prop);
            } else {
                log.info("restore {}", prop);
                System.setProperty(prop, value);
            }
        });
        systemProp.clear();
    }

    @After
    public void tearDown() throws Exception {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        ResourceStore.clearCache(kylinConfig);
        System.clearProperty("kylin.job.scheduler.poll-interval-second");

        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        restoreAllSystemProp();
    }

    String getFolder(String subFolder) {
        return IT_SQL_KAP_DIR + File.separator + subFolder;
    }

    protected void executeTestScenario(TestScenario... testScenarios) throws Exception {
        executeTestScenario(true, testScenarios);
    }

    private void executeTestScenario(boolean needCompareLayouts, TestScenario... testScenarios) throws Exception {

        // 1. execute auto-modeling propose
        final NSmartMaster smartMaster = proposeWithSmartMaster(testScenarios, getProject());

        final Map<String, CompareEntity> compareMap = collectCompareEntity(smartMaster);

        try (KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf))) {
            // 2. execute cube building
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            // dump metadata for debugging
            // dumpMetadata();

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
            });
        } finally {
            FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        }

        // 4. compare layout propose result and query cube result
        RecAndQueryCompareUtil.computeCompareRank(kylinConfig, getProject(), compareMap);
        // 5. check layout
        assertOrPrintCmpResult(compareMap, needCompareLayouts);

        // 6. summary info
        final Map<AccelerationMatchedLevel, AtomicInteger> rankInfoMap = RecAndQueryCompareUtil
                .summarizeRankInfo(compareMap);
        StringBuilder sb = new StringBuilder();
        sb.append("All used queries: ").append(compareMap.size()).append('\n');
        rankInfoMap.forEach((key, value) -> sb.append(key).append(": ").append(value).append("\n"));
        System.out.println(sb);
    }

    private void dumpMetadata() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val metadataUrlPrefix = config.getMetadataUrlPrefix();
        val metadataUrl = metadataUrlPrefix + "/metadata";
        FileUtils.deleteQuietly(new File(metadataUrl));
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val outputConfig = KylinConfig.createKylinConfig(config);
        outputConfig.setMetadataUrl(metadataUrlPrefix);
        ResourceStore.createMetadataStore(outputConfig).dump(resourceStore);
    }

    private void assertOrPrintCmpResult(Map<String, CompareEntity> compareMap, boolean needCompareLayouts) {
        // print details
        compareMap.forEach((key, value) -> {
            final String sqlPattern = QueryPatternUtil.normalizeSQLPattern(key);
            log.debug("**start comparing the SQL: \n{} \n accelerate layout info**", key);
            if (!excludedSqlPatterns.contains(sqlPattern) && needCompareLayouts) {
                Assert.assertEquals(value.getAccelerateLayouts(), value.getQueryUsedLayouts());
            } else {
                System.out.println(value.toString() + '\n');
            }
        });
    }

    private Set<String> loadWhiteListSqlPatterns() throws IOException {

        Set<String> result = Sets.newHashSet();
        final String folder = getFolder("unchecked_layout_list");
        File[] files = new File(folder).listFiles();
        if (files == null || files.length == 0) {
            return result;
        }

        String[] fileContentArr = new String(getFileBytes(files[0])).split(FILE_SEPARATOR);
        final List<String> fileNames = Arrays.stream(fileContentArr)
                .filter(name -> !name.startsWith("-") && name.length() > 0) //
                .collect(Collectors.toList());
        final List<Pair<String, String>> queries = Lists.newArrayList();
        for (String name : fileNames) {
            File tmp = new File(IT_SQL_KAP_DIR + "/" + name);
            final String sql = new String(getFileBytes(tmp));
            queries.add(new Pair<>(tmp.getCanonicalPath(), sql));
        }

        queries.forEach(pair -> {
            String sql = pair.getSecond(); // origin sql
            result.addAll(changeJoinType(sql));

            // add limit
            if (!sql.toLowerCase().contains("limit ")) {
                result.addAll(changeJoinType(sql + " limit 5"));
            }
        });

        return result;
    }

    private Set<String> changeJoinType(String sql) {
        Set<String> patterns = Sets.newHashSet();
        for (JoinType joinType : JoinType.values()) {
            final String rst = KylinTestBase.changeJoinType(sql, joinType.name());
            patterns.add(QueryPatternUtil.normalizeSQLPattern(rst));
        }

        return patterns;
    }

    private byte[] getFileBytes(File whiteListFile) throws IOException {
        final Long fileLength = whiteListFile.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try (FileInputStream inputStream = new FileInputStream(whiteListFile)) {
            final int read = inputStream.read(fileContent);
            Preconditions.checkState(read != -1);
        }
        return fileContent;
    }

    NSmartMaster proposeWithSmartMaster(TestScenario[] testScenarios, String project) throws IOException {

        List<String> sqlList = collectQueries(testScenarios);

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));

        String[] sqls = sqlList.toArray(new String[0]);
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, project, sqls);
        smartMaster.runAll();
        return smartMaster;
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

    private void normalizeSql(JoinType joinType, List<Pair<String, String>> queries) {
        queries.forEach(pair -> {
            String transformedQuery = QueryUtil.massageSql(pair.getSecond(), getProject(), 0, 0, "DEFAULT");
            transformedQuery = QueryUtil.removeCommentInSql(transformedQuery);
            String tmp = KylinTestBase.changeJoinType(transformedQuery, joinType.name());
            // tmp = QueryPatternUtil.normalizeSQLPattern(tmp); // something wrong with sqlPattern, skip this step
            pair.setSecond(tmp);
        });
    }

    List<String> collectQueries(TestScenario... tests) throws IOException {
        List<String> allQueries = Lists.newArrayList();
        for (TestScenario test : tests) {
            List<Pair<String, String>> queries = fetchQueries(test.folderName, test.fromIndex, test.toIndex);
            normalizeSql(test.joinType, queries);
            test.queries = test.exclusionList == null ? queries : NExecAndComp.doFilter(queries, test.exclusionList);
            allQueries.addAll(test.queries.stream().map(Pair::getSecond).collect(Collectors.toList()));
        }
        return allQueries;
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
        List<Pair<String, String>> queries;

        TestScenario(String folderName) {
            this(CompareLevel.SAME, folderName);
        }

        TestScenario(String folderName, int fromIndex, int toIndex) {
            this(CompareLevel.SAME, folderName, fromIndex, toIndex);
        }

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

        public void execute(boolean needCompareLayouts) throws Exception {
            executeTestScenario(needCompareLayouts, this);
        }

        public void execute() throws Exception {
            executeTestScenario(this);
        }

    } // end TestScenario

    private Map<String, CompareEntity> collectCompareEntity(NSmartMaster smartMaster) {
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
