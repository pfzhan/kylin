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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
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
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.newten.auto.NAutoTestBase;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.tool.util.ZipFileUtil;
import io.kyligence.kap.utils.AccelerationContextUtil;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class NSuggestTestBase extends NLocalWithSparkSessionTest {

    protected static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/";

    protected KylinConfig kylinConfig;
    private JdbcTemplate jdbcTemplate;
    protected static Set<String> excludedSqlPatterns = Sets.newHashSet();

    @Before
    public void setup() throws Exception {
        super.init();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        new JdbcRawRecStore(getTestConfig());
        kylinConfig = getTestConfig();
    }

    @After
    public void tearDown() throws Exception {
        NDefaultScheduler.destroyInstance();
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        }
        super.cleanupTestMetadata();
        ResourceStore.clearCache(kylinConfig);
        excludedSqlPatterns.clear();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");

        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    public Set<String> loadWhiteListPatterns() throws IOException {
        log.info("override loadWhiteListSqlPatterns in NAutoBuildAndQueryTest");

        Set<String> result = Sets.newHashSet();
        final String folder = getFolder("query/unchecked_layout_list");
        File[] files = new File(folder).listFiles();
        if (files == null || files.length == 0) {
            return result;
        }

        String[] fileContentArr = new String(getFileBytes(files[0])).split(System.getProperty("line.separator"));
        final List<String> fileNames = Arrays.stream(fileContentArr)
                .filter(name -> !name.startsWith("-") && name.length() > 0) //
                .collect(Collectors.toList());
        final List<Pair<String, String>> queries = Lists.newArrayList();
        for (String name : fileNames) {
            File tmp = new File(NAutoTestBase.IT_SQL_KAP_DIR + "/" + name);
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

    @Override
    public String getProject() {
        return "newten";
    }

    protected String getFolder(String subFolder) {
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

        public TestScenario(String folderName) {
            this(NExecAndComp.CompareLevel.SAME, folderName);
        }

        TestScenario(String folderName, int fromIndex, int toIndex) {
            this(NExecAndComp.CompareLevel.SAME, folderName, fromIndex, toIndex);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, String folder) {
            this(compareLevel, JoinType.DEFAULT, folder);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, JoinType joinType, String folder) {
            this(compareLevel, joinType, false, folder, 0, 0, null);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, String folder, int fromIndex, int toIndex) {
            this(compareLevel, JoinType.DEFAULT, false, folder, fromIndex, toIndex, null);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, String folder, JoinType joinType, int fromIndex,
                int toIndex) {
            this(compareLevel, joinType, false, folder, fromIndex, toIndex, null);
        }

        TestScenario(NExecAndComp.CompareLevel compareLevel, String folder, Set<String> exclusionList) {
            this(compareLevel, JoinType.DEFAULT, false, folder, 0, 0, exclusionList);
        }

        public TestScenario(NExecAndComp.CompareLevel compareLevel, boolean isLimit, String folder) {
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

        public void execute() throws Exception {
            executeTestScenario(this);
        }

    } // end TestScenario

    protected abstract Map<String, RecAndQueryCompareUtil.CompareEntity> executeTestScenario(
            TestScenario... testScenarios) throws Exception;

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

        boolean noBuild = Boolean.parseBoolean(System.getProperty("noBuild", "false"));
        if (noBuild) {
            reuseBuildData();
            return;
        }
        for (IRealization realization : projectManager.listAllRealizations(proj)) {
            NDataflow df = (NDataflow) realization;
            Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
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
                NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(readySegments),
                        Sets.newLinkedHashSet(layouts), "ADMIN", null);
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
                    merger.mergeAfterCatchup(df.getUuid(), segIds, layoutIds, buildStore, null);
                }

                SchemaProcessor.checkSchema(SparderEnv.getSparkSession(), df.getUuid(), proj);
            }
        }
        persistBuildData();
    }

    private void reuseBuildData() {
        val element = Arrays.stream(Thread.currentThread().getStackTrace())
                .filter(ele -> ele.getClassName().startsWith("io.kyligence.kap")) //
                .reduce((first, second) -> second) //
                .get();
        val inputFolder = new File(kylinConfig.getMetadataUrlPrefix()).getParentFile();
        val outputFolder = new File(inputFolder.getParentFile(),
                element.getClassName() + "." + element.getMethodName());
        try {
            FileUtils.deleteQuietly(outputFolder);
            ZipFileUtil.decompressZipFile(outputFolder.getAbsolutePath() + ".zip",
                    outputFolder.getParentFile().getAbsolutePath());
            FileUtils.copyDirectory(new File(outputFolder, "working-dir"), new File(inputFolder, "working-dir"));
            log.info("reuse data succeed for {}", outputFolder);
        } catch (IOException e) {
            throw new RuntimeException("Reuse " + outputFolder.getName() + " failed", e);
        }

        val buildConfig = KylinConfig.createKylinConfig(kylinConfig);
        buildConfig.setMetadataUrl(outputFolder.getAbsolutePath() + "/metadata");
        val buildStore = ResourceStore.getKylinMetaStore(buildConfig);
        val store = ResourceStore.getKylinMetaStore(kylinConfig);
        for (String key : store.listResourcesRecursively("/" + getProject())) {
            store.deleteResource(key);
        }
        for (String key : buildStore.listResourcesRecursively("/" + getProject())) {
            val raw = buildStore.getResource(key);
            store.deleteResource(key);
            store.putResourceWithoutCheck(key, raw.getByteSource(), System.currentTimeMillis(), 100);
        }
        FileUtils.deleteQuietly(outputFolder);
    }

    private void persistBuildData() {
        if (!Boolean.parseBoolean(System.getProperty("persistBuild", "false"))) {
            return;
        }

        val element = Arrays.stream(Thread.currentThread().getStackTrace())
                .filter(ele -> ele.getClassName().startsWith("io.kyligence.kap")) //
                .reduce((first, second) -> second) //
                .get();
        try {
            dumpMetadata();
            val inputFolder = new File(kylinConfig.getMetadataUrlPrefix()).getParentFile();
            val outputFolder = new File(inputFolder.getParentFile(),
                    element.getClassName() + "." + element.getMethodName());
            FileUtils.deleteQuietly(outputFolder);
            FileUtils.deleteQuietly(new File(outputFolder.getAbsolutePath() + ".zip"));
            FileUtils.copyDirectory(inputFolder, outputFolder);
            ZipFileUtil.compressZipFile(outputFolder.getAbsolutePath(), outputFolder.getAbsolutePath() + ".zip");
            log.info("build data succeed for {}", outputFolder.getName());
            FileUtils.deleteQuietly(outputFolder);
        } catch (Exception e) {
            log.warn("build data failed", e);
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

    protected List<String> collectQueries(TestScenario... tests) throws IOException {
        List<String> allQueries = Lists.newArrayList();
        List<Pair<String, String>> tmpQueryList = Lists.newArrayList();
        for (TestScenario test : tests) {
            List<Pair<String, String>> queries = fetchQueries(test.folderName, test.getFromIndex(), test.getToIndex());
            normalizeSql(test.joinType, queries);
            tmpQueryList.addAll(queries);
            test.queries = test.getExclusionList() == null ? queries
                    : NExecAndComp.doFilter(queries, test.getExclusionList());
            allQueries.addAll(test.queries.stream().map(Pair::getSecond).collect(Collectors.toList()));
        }
        Map<Integer, List<Pair<String, String>>> classifiedMap = classify(tmpQueryList);

        return allQueries;
    }

    private Map<Integer, List<Pair<String, String>>> classify(List<Pair<String, String>> queryList) {
        Map<Integer, List<Pair<String, String>>> result = Maps.newHashMap();
        queryList.forEach(pair -> {
            String sqlContent = pair.getSecond();
            int times = StringUtils.countMatches(sqlContent.toLowerCase(), " join ");
            result.putIfAbsent(times, Lists.newArrayList());
            result.get(times).add(pair);
        });
        return result;
    }

    protected List<Pair<String, String>> fetchQueries(String subFolder, int fromIndex, int toIndex) throws IOException {
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

    protected void assertOrPrintCmpResult(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap) {
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

    protected Set<String> changeJoinType(String sql) {
        Set<String> patterns = Sets.newHashSet();
        for (JoinType joinType : JoinType.values()) {
            final String rst = KylinTestBase.changeJoinType(sql, joinType.name());
            patterns.add(QueryPatternUtil.normalizeSQLPattern(rst));
        }

        return patterns;
    }

    protected byte[] getFileBytes(File whiteListFile) throws IOException {
        final Long fileLength = whiteListFile.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try (FileInputStream inputStream = new FileInputStream(whiteListFile)) {
            final int read = inputStream.read(fileContent);
            Preconditions.checkState(read != -1);
        }
        return fileContent;
    }

    protected abstract NSmartMaster proposeWithSmartMaster(String project, TestScenario... testScenarios)
            throws IOException;

    protected void buildAndCompare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            TestScenario... testScenarios) throws Exception {
        try {
            // 2. execute cube building
            long startTime = System.currentTimeMillis();
            buildAllCubes(kylinConfig, getProject());
            log.info("build cube cost {} ms", System.currentTimeMillis() - startTime);

            // dump metadata for debugging
            dumpMetadata();

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
            FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        }
    }

    public static Consumer<AbstractContext> smartUtHook = AccelerationContextUtil::onlineModel;
}
