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
import java.util.function.Consumer;
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
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSuggestTestBase extends NLocalWithSparkSessionTest {

    static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/";

    protected KylinConfig kylinConfig;
    protected static Set<String> excludedSqlPatterns = Sets.newHashSet();

    @Before
    public void setup() throws Exception {
        super.init();
        kylinConfig = getTestConfig();
        if (CollectionUtils.isEmpty(excludedSqlPatterns)) {
            excludedSqlPatterns = loadWhiteListSqlPatterns();
        }
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

    protected Set<String> loadWhiteListSqlPatterns() throws IOException {
        return Sets.newHashSet();
    }

    protected void executeTestScenario(TestScenario... testScenarios) throws Exception {
        executeTestScenario(false, testScenarios);
    }

    @Override
    public String getProject() {
        return "newten";
    }

    String getFolder(String subFolder) {
        return IT_SQL_KAP_DIR + File.separator + subFolder;
    }

    protected void dumpMetadata() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val metadataUrlPrefix = config.getMetadataUrlPrefix();
        val metadataUrl = metadataUrlPrefix + "/metadata";
        FileUtils.deleteQuietly(new File(metadataUrl));
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val outputConfig = KylinConfig.createKylinConfig(config);
        outputConfig.setMetadataUrl(metadataUrlPrefix);
        MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
    }

    @Getter
    @Setter
    public class TestScenario {

        Set<Pair<String, Long>> removeLayouts;
        String folderName;
        private NExecAndComp.CompareLevel compareLevel;
        JoinType joinType;
        private int fromIndex;
        private int toIndex;
        private boolean isLimit;
        private Set<String> exclusionList;
        private boolean isDynamicSql = false;

        // value when execute
        List<Pair<String, String>> queries;

        TestScenario(String folderName) {
            this(NExecAndComp.CompareLevel.SAME, folderName);
        }

        TestScenario(String folderName, int fromIndex, int toIndex) {
            this(NExecAndComp.CompareLevel.SAME, folderName, fromIndex, toIndex);
        }

        TestScenario(NExecAndComp.CompareLevel compareLevel, String folder) {
            this(compareLevel, JoinType.DEFAULT, folder);
        }

        TestScenario(NExecAndComp.CompareLevel compareLevel, JoinType joinType, String folder) {
            this(compareLevel, joinType, false, folder, 0, 0, null);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, String folder, int fromIndex, int toIndex) {
            this(compareLevel, JoinType.DEFAULT, false, folder, fromIndex, toIndex, null);
        }

        TestScenario(NExecAndComp.CompareLevel compareLevel, String folder, Set<String> exclusionList) {
            this(compareLevel, JoinType.DEFAULT, false, folder, 0, 0, exclusionList);
        }

        TestScenario(NExecAndComp.CompareLevel compareLevel, boolean isLimit, String folder) {
            this(compareLevel, JoinType.DEFAULT, isLimit, folder, 0, 0, null);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, JoinType joinType, boolean isLimit,
                String folderName, int fromIndex, int toIndex, Set<String> exclusionList) {
            this.compareLevel = compareLevel;
            this.folderName = folderName;
            this.joinType = joinType;
            this.isLimit = isLimit;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.exclusionList = exclusionList;
            this.removeLayouts = Sets.newHashSet();
        }

        public Map<String, RecAndQueryCompareUtil.CompareEntity> execute(boolean recordFQ) throws Exception {
            return executeTestScenario(recordFQ, this);
        }

        public void execute() throws Exception {
            executeTestScenario(this);
        }

    } // end TestScenario

    protected Map<String, RecAndQueryCompareUtil.CompareEntity> executeTestScenario(boolean recordFQ,
            TestScenario... testScenarios) throws Exception {
        return Maps.newHashMap();
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

    protected Map<String, RecAndQueryCompareUtil.CompareEntity> collectCompareEntity(NSmartMaster smartMaster) {
        Map<String, RecAndQueryCompareUtil.CompareEntity> map = Maps.newHashMap();
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((sql, accelerateInfo) -> {
            map.putIfAbsent(sql, new RecAndQueryCompareUtil.CompareEntity());
            final RecAndQueryCompareUtil.CompareEntity entity = map.get(sql);
            entity.setAccelerateInfo(accelerateInfo);
            entity.setAccelerateLayouts(RecAndQueryCompareUtil.writeQueryLayoutRelationAsString(kylinConfig,
                    getProject(), accelerateInfo.getRelatedLayouts()));
            entity.setSql(sql);
        });
        return map;
    }

    List<String> collectQueries(TestScenario... tests) throws IOException {
        List<String> allQueries = Lists.newArrayList();
        for (TestScenario test : tests) {
            List<Pair<String, String>> queries = fetchQueries(test.folderName, test.getFromIndex(), test.getToIndex());
            normalizeSql(test.joinType, queries);
            test.queries = test.getExclusionList() == null ? queries
                    : NExecAndComp.doFilter(queries, test.getExclusionList());
            allQueries.addAll(test.queries.stream().map(Pair::getSecond).collect(Collectors.toList()));
        }
        return allQueries;
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
            String tmp = KylinTestBase.changeJoinType(pair.getSecond(), joinType.name());
            // tmp = QueryPatternUtil.normalizeSQLPattern(tmp); // something wrong with sqlPattern, skip this step
            pair.setSecond(tmp);
        });
    }

    void assertOrPrintCmpResult(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap) {
        // print details
        compareMap.forEach((key, value) -> {
            if (value.getLevel().equals(RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY)) {
                return;
            }
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

    protected NSmartMaster proposeWithSmartMaster(String project, TestScenario... testScenarios) throws IOException {
        return proposeWithSmartMaster(project, NSmartMaster::runAll, testScenarios);
    }

    protected NSmartMaster proposeWithSmartMaster(String project, Consumer<NSmartMaster> smartRunner,
            TestScenario... testScenarios) throws IOException {

        List<String> sqlList = collectQueries(testScenarios);

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));

        String[] sqls = sqlList.toArray(new String[0]);
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), project, sqls);
        smartRunner.accept(smartMaster);
        return smartMaster;
    }

    protected void buildAndCompare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            TestScenario... testScenarios) throws Exception {
        try {
            // 2. execute cube building
            long startTime = System.currentTimeMillis();
            buildAllCubes(kylinConfig, getProject());
            log.debug("build cube cost {} s", System.currentTimeMillis() - startTime);

            // dump metadata for debugging
            // dumpMetadata();

            // 3. validate results between SparkSQL and cube
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            startTime = System.currentTimeMillis();
            Arrays.stream(testScenarios).forEach(testScenario -> {
                populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
                if (testScenario.isLimit()) {
                    NExecAndComp.execLimitAndValidateNew(testScenario.queries, getProject(), JoinType.DEFAULT.name(),
                            compareMap);
                } else if (testScenario.isDynamicSql()) {
                    NExecAndComp.execAndCompareDynamic(testScenario.queries, getProject(),
                            testScenario.getCompareLevel(), testScenario.joinType.name(), compareMap);
                } else {
                    NExecAndComp.execAndCompareNew(testScenario.queries, getProject(), testScenario.getCompareLevel(),
                            testScenario.joinType.name(), compareMap);
                }
            });
            log.debug("compare result cost {} s", System.currentTimeMillis() - startTime);
        } finally {
            FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        }
    }
}
