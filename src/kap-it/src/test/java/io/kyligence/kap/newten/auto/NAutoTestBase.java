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
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.execution.utils.SchemaProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.CompareEntity;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NAutoTestBase extends NLocalWithSparkSessionTest {

    static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/";

    protected KylinConfig kylinConfig;
    private static Set<String> excludedSqlPatterns = Sets.newHashSet();

    @Before
    public void setup() throws Exception {
        super.init();
        kylinConfig = getTestConfig();
        if (CollectionUtils.isEmpty(excludedSqlPatterns)) {
            excludedSqlPatterns = loadWhiteListSqlPatterns();
        }

    }

    @Override
    public String getProject() {
        return "newten";
    }

    @After
    public void tearDown() throws Exception {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        ResourceStore.clearCache(kylinConfig);
        excludedSqlPatterns.clear();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");

        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        restoreAllSystemProp();
    }

    String getFolder(String subFolder) {
        return IT_SQL_KAP_DIR + File.separator + subFolder;
    }

    protected void executeTestScenario(TestScenario... testScenarios) throws Exception {
        executeTestScenario(false, testScenarios);
    }

    private Map<String, CompareEntity> executeTestScenario(boolean recordFQ, TestScenario... testScenarios)
            throws Exception {

        // 1. execute auto-modeling propose
        final NSmartMaster smartMaster = proposeWithSmartMaster(getProject(), testScenarios);

        final Map<String, CompareEntity> compareMap = collectCompareEntity(smartMaster);

        try {
            // 2. execute cube building
            buildAllCubes(kylinConfig, getProject());

            // dump metadata for debugging
            // dumpMetadata();

            // 3. validate results between SparkSQL and cube
            Arrays.stream(testScenarios).forEach(testScenario -> {
                populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
                if (testScenario.isLimit) {
                    NExecAndComp.execLimitAndValidateNew(testScenario.queries, getProject(), JoinType.DEFAULT.name(),
                            compareMap);
                } else if (testScenario.isDynamicSql) {
                    NExecAndComp.execAndCompareDynamic(testScenario, getProject(), testScenario.joinType.name(),
                            compareMap);
                } else {
                    NExecAndComp.execAndCompareNew(testScenario.queries, getProject(), testScenario.compareLevel,
                            testScenario.joinType.name(), compareMap);
                }
            });
        } finally {
            FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        }

        // 4. compare layout propose result and query cube result
        RecAndQueryCompareUtil.computeCompareRank(kylinConfig, getProject(), compareMap);
        // 5. check layout
        assertOrPrintCmpResult(compareMap);

        // 6. summary info
        val rankInfoMap = RecAndQueryCompareUtil.summarizeRankInfo(compareMap);
        StringBuilder sb = new StringBuilder();
        sb.append("All used queries: ").append(compareMap.size()).append('\n');
        rankInfoMap.forEach((key, value) -> sb.append(key).append(": ").append(value).append("\n"));
        System.out.println(sb);
        return compareMap;
    }

    private void dumpMetadata() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val metadataUrlPrefix = config.getMetadataUrlPrefix();
        val metadataUrl = metadataUrlPrefix + "/metadata";
        FileUtils.deleteQuietly(new File(metadataUrl));
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val outputConfig = KylinConfig.createKylinConfig(config);
        outputConfig.setMetadataUrl(metadataUrlPrefix);
        MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
    }

    private void assertOrPrintCmpResult(Map<String, CompareEntity> compareMap) {
        // print details
        compareMap.forEach((key, value) -> {
            final String sqlPattern = value.getFilePath().contains("/sql_parentheses_escape/") //
                    ? key // sql in fold of sql_parentheses_escape cannot normalize sqlPattern directly
                    : QueryPatternUtil.normalizeSQLPattern(key);
            log.debug("** start comparing the SQL: {} **", value.getFilePath());
            if (!excludedSqlPatterns.contains(sqlPattern) && !value.ignoredCompareLevel()) {
                Assert.assertEquals(
                        "something unexpected happened when comparing result of sql: " + value.getFilePath(),
                        value.getAccelerateLayouts(), value.getQueryUsedLayouts());
            } else {
                log.info(value.toString() + '\n');
            }
        });
    }

    protected Set<String> loadWhiteListSqlPatterns() throws IOException {
        return Sets.newHashSet();
    }

    Set<String> changeJoinType(String sql) {
        Set<String> patterns = Sets.newHashSet();
        for (JoinType joinType : JoinType.values()) {
            final String rst = KylinTestBase.changeJoinType(sql, joinType.name());
            patterns.add(QueryPatternUtil.normalizeSQLPattern(rst));
        }

        return patterns;
    }

    byte[] getFileBytes(File whiteListFile) throws IOException {
        final Long fileLength = whiteListFile.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try (FileInputStream inputStream = new FileInputStream(whiteListFile)) {
            final int read = inputStream.read(fileContent);
            Preconditions.checkState(read != -1);
        }
        return fileContent;
    }

    NSmartMaster proposeWithSmartMaster(String project, TestScenario... testScenarios) throws IOException {

        List<String> sqlList = collectQueries(testScenarios);

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));

        String[] sqls = sqlList.toArray(new String[0]);
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), project, sqls);
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
            String transformedQuery = QueryUtil.removeCommentInSql(pair.getSecond());
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

    public void buildAllCubes(KylinConfig kylinConfig, String proj) throws InterruptedException {
        kylinConfig.clearManagers();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        NExecutableManager execMgr = NExecutableManager.getInstance(kylinConfig, proj);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, proj);

        for (IRealization realization : projectManager.listAllRealizations(proj)) {
            NDataflow df = (NDataflow) realization;
            Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
            NDataSegment oneSeg;
            List<LayoutEntity> layouts;
            boolean isAppend = false;
            if (readySegments.isEmpty()) {
                oneSeg = dataflowManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
                layouts = df.getIndexPlan().getAllLayouts();
                isAppend = true;
                readySegments.add(oneSeg);
            } else {
                oneSeg = readySegments.getFirstSegment();
                layouts = df.getIndexPlan().getAllLayouts().stream()
                        .filter(c -> !oneSeg.getLayoutsMap().containsKey(c.getId())).collect(Collectors.toList());
            }

            // create cubing job
            if (!layouts.isEmpty()) {
                NSparkCubingJob job = NSparkCubingJob.create(
                        org.spark_project.guava.collect.Sets.newHashSet(readySegments),
                        org.spark_project.guava.collect.Sets.newLinkedHashSet(layouts), "ADMIN");
                execMgr.addJob(job);
                while (true) {
                    Thread.sleep(500);
                    ExecutableState status = job.getStatus();
                    if (!status.isProgressing()) {
                        if (status == ExecutableState.ERROR) {
                            throw new IllegalStateException("Failed to execute job. " + job);
                        } else
                            break;
                    }
                }
                val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
                AfterBuildResourceMerger merger = new AfterBuildResourceMerger(kylinConfig, proj);
                val layoutIds = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
                if (isAppend) {
                    merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), layoutIds, buildStore);
                } else {
                    val segIds = readySegments.stream().map(nDataSegment -> nDataSegment.getId())
                            .collect(Collectors.toSet());
                    merger.mergeAfterCatchup(df.getUuid(), segIds, layoutIds, buildStore);
                }

                SchemaProcessor.checkSchema(SparderEnv.getSparkSession(), df.getUuid(), proj);
            }
        }
    }

    public class TestScenario {

        String folderName;
        @Getter
        private CompareLevel compareLevel;
        JoinType joinType;
        private int fromIndex;
        private int toIndex;
        private boolean isLimit;
        private Set<String> exclusionList;
        @Getter
        @Setter
        private boolean isDynamicSql = false;

        // value when execute
        @Getter
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

        public TestScenario(CompareLevel compareLevel, JoinType joinType, boolean isLimit, String folderName,
                int fromIndex, int toIndex, Set<String> exclusionList) {
            this.compareLevel = compareLevel;
            this.folderName = folderName;
            this.joinType = joinType;
            this.isLimit = isLimit;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.exclusionList = exclusionList;
        }

        public Map<String, CompareEntity> execute(boolean recordFQ) throws Exception {
            return executeTestScenario(recordFQ, this);
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
