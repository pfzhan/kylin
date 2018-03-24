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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalSparkWithMetaTest;
import io.kyligence.kap.spark.KapSparkSession;

public class NITQueryTest extends NLocalSparkWithMetaTest {
    private static final Logger logger = LoggerFactory.getLogger(NITQueryTest.class);
    private static KylinConfig kylinConfig;
    private static KapSparkSession kapSparkSession;
    private static final String CSV_TABLE_DIR = "../examples/test_metadata/data/%s.csv";
    private static final String DEFAULT_PROJECT = "newten";
    private static final String IT_SQL_BASE_DIR = "../../kylin/kylin-it/src/test/resources/query";
    private static final String[] EXCLUDE_SQL_LIST = {};

    enum CompareLevel {
        UNAVAILABLE, AVAILABLE, COMPARABLE
    }

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");
        DefaultScheduler scheduler = DefaultScheduler.getInstance();
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

        //if (kapSparkSession != null)
            //kapSparkSession.close();

        DefaultScheduler.destroyInstance();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("org.xerial.snappy.lib.name");
    }

    @Test
    public void testBasic() throws Exception {
        testScenario("sql", CompareLevel.COMPARABLE);
    }

    @Test
    public void testCache() throws Exception {
        testScenario("sql_cache", CompareLevel.COMPARABLE);
    }

    @Test
    public void testCasewhen() throws Exception {
        testScenario("sql_casewhen", CompareLevel.COMPARABLE);
    }

    @Test
    public void testDatetime() throws Exception {
        testScenario("sql_datetime", CompareLevel.COMPARABLE);
    }

    @Test
    public void testDerived() throws Exception {
        testScenario("sql_derived", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testDistinct() throws Exception {
        testScenario("sql_distinct", CompareLevel.AVAILABLE);
    }

    @Test
    public void testDistinctDim() throws Exception {
        testScenario("sql_distinct_dim", CompareLevel.AVAILABLE);
    }

    @Test
    public void testDistinctPrecisely() throws Exception {
        testScenario("sql_distinct_precisely", CompareLevel.AVAILABLE);
    }

    @Test
    public void testDynamic() throws Exception {
        testScenario("sql_dynamic", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testExtendedColumn() throws Exception {
        testScenario("sql_extended_column", CompareLevel.COMPARABLE);
    }

    @Test
    public void testGrouping() throws Exception {
        testScenario("sql_grouping", CompareLevel.COMPARABLE);
    }

    @Test
    public void testH2Uncapable() throws Exception {
        testScenario("sql_h2_uncapable", CompareLevel.COMPARABLE);
    }

    @Test
    public void testHive() throws Exception {
        testScenario("sql_hive", CompareLevel.COMPARABLE);
    }

    @Test
    public void testIntersectCount() throws Exception {
        testScenario("sql_intersect_count", CompareLevel.AVAILABLE);
    }

    @Test
    public void testInvalid() throws Exception {
        testScenario("sql_invalid", CompareLevel.COMPARABLE);
    }

    @Test
    public void testJoin() throws Exception {
        testScenario("sql_join", CompareLevel.AVAILABLE);
    }

    @Test
    public void testLike() throws Exception {
        testScenario("sql_like", CompareLevel.COMPARABLE);
    }

    @Test
    public void testLimit() throws Exception {
        testScenario("sql_limit", CompareLevel.AVAILABLE);
    }

    @Test
    public void testLookup() throws Exception {
        testScenario("sql_lookup", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testMassin() throws Exception {
        testScenario("sql_massin", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testMassinDistinct() throws Exception {
        testScenario("sql_massin_distinct", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testMultiModel() throws Exception {
        testScenario("sql_multi_model", CompareLevel.COMPARABLE);
    }

    @Test
    public void testOrderBy() throws Exception {
        testScenario("sql_orderby", CompareLevel.AVAILABLE);
    }

    @Test
    public void testPercentile() throws Exception {
        testScenario("sql_percentile", CompareLevel.AVAILABLE);
    }

    @Test
    public void testRaw() throws Exception {
        testScenario("sql_raw", CompareLevel.UNAVAILABLE);
    }

    // The strict join condition cause records lost.
    @Test
    public void testSnowFlake() throws Exception {
        testScenario("sql_snowflake", CompareLevel.AVAILABLE);
    }

    @Test
    public void testStreaming() throws Exception {
        testScenario("sql_streaming", CompareLevel.UNAVAILABLE);
    }

    // Automodeling problem, Yifan focus on it.
    @Test
    public void testSubQuery() throws Exception {
        testScenario("sql_subquery", CompareLevel.AVAILABLE);
    }

    // different results
    @Test
    public void testTableau() throws Exception {
        testScenario("sql_tableau", CompareLevel.AVAILABLE);
    }

    @Test
    public void testTimeout() throws Exception {
        testScenario("sql_timeout", CompareLevel.AVAILABLE);
    }

    @Test
    public void testTopn() throws Exception {
        testScenario("sql_topn", CompareLevel.COMPARABLE);
    }

    // not supported
    @Test
    public void testUnion() throws Exception {
        testScenario("sql_union", CompareLevel.COMPARABLE);
    }

    @Test
    public void testVerifyContent() throws Exception {
        testScenario("sql_verifyContent", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testVerifyCount() throws Exception {
        testScenario("sql_verifyCount", CompareLevel.UNAVAILABLE);
    }

    @Test
    public void testWindow() throws Exception {
        testScenario("sql_window", CompareLevel.AVAILABLE);
    }

    @Test
    public void testTableauProbing() throws Exception {
        testScenario("tableau_probing", CompareLevel.UNAVAILABLE);
    }

    private void testScenario(String name, CompareLevel compareLevel) throws Exception {

        switch (compareLevel) {
        case UNAVAILABLE:
            break;
        case AVAILABLE:
            availabilityTest(name);
            break;
        case COMPARABLE:
            comparisonTest(name);
        default:
            break;
        }
    }

    private void availabilityTest(String name) throws Exception {
        Map<String, String> queries = fetchPartialQueries(name, 0, 0);
        Assert.assertTrue(queries.size() > 0);
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        for (String query : queries.values()) {
            kapSparkSession.collectQueries(query);
        }

        kapSparkSession.speedUp();

        // Step2. Query cube and query SparkSQL respectively
        kapSparkSession.close();
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);

        // Query from Cube
        for (String query : queries.values()) {
            Dataset<Row> ret = kapSparkSession.queryFromCube(query);
            ret.show();
        }
    }

    private void comparisonTest(String name) throws Exception {
        Map<String, String> queries = fetchPartialQueries(name, 60, 61);
        Assert.assertTrue(queries.size() > 0);
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        for (String query : queries.values()) {
            kapSparkSession.collectQueries(query);
        }

        kapSparkSession.speedUp();

        // Step2. Query cube and query SparkSQL respectively
        kapSparkSession.close();
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);

        // Query from Cube
        List<Dataset> resultsOfCube = Lists.newArrayList();
        for (String query : queries.values()) {
            Dataset<Row> ret = kapSparkSession.queryFromCube(query);
            resultsOfCube.add(ret);
        }

        // Query from SparkSQL
        prepareBeforeSparkSql();
        List<Dataset> resultsOfSparkSql = Lists.newArrayList();
        for (String sql : queries.values()) {
            for (String converterName : kylinConfig.getPushDownConverterClassNames()) {
                IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(converterName);
                String converted = converter.convert(sql, DEFAULT_PROJECT, "DEFAULT", false);
                if (!sql.equals(converted)) {
                    logger.info("the query is converted to {} after applying converter {}", converted, converterName);
                    sql = converted;
                }
            }
            // Table schema comes from csv and DATABASE.TABLE is not supported.
            String sqlForSpark = sql.replaceAll("edw.", "").replaceAll("default.", "").replaceAll("EDW.", "")
                    .replaceAll("DEFAULT", "");
            Dataset<Row> ret = kapSparkSession.querySparkSql(sqlForSpark);
            resultsOfSparkSql.add(ret);
        }

        // Step3. Validate results between sparksql and cube
        compareResults(resultsOfSparkSql, resultsOfCube);

    }

    private void compareResults(List<Dataset> r1, List<Dataset> r2) {
        Assert.assertTrue(r1.size() == r2.size());
        for (int i = 0; i < r1.size(); i++) {
            Dataset<Row> queryRet1 = r1.get(i);
            Dataset<Row> queryRet2 = r2.get(i);
            Preconditions.checkArgument(queryRet1 != null);
            Preconditions.checkArgument(queryRet2 != null);
            queryRet1.persist();
            queryRet2.persist();
            Assert.assertEquals(0, queryRet1.except(queryRet2).count());
            Assert.assertEquals(0, queryRet2.except(queryRet1).count());
            queryRet1.unpersist();
            queryRet2.unpersist();
        }
    }

    private void prepareBeforeSparkSql() {
        ProjectInstance projectInstance = ProjectManager.getInstance(kylinConfig).getProject(DEFAULT_PROJECT);
        Preconditions.checkArgument(projectInstance != null);
        for (String table : projectInstance.getTables()) {
            TableDesc tableDesc = TableMetadataManager.getInstance(kylinConfig).getTableDesc(table, DEFAULT_PROJECT);
            ColumnDesc[] columns = tableDesc.getColumns();
            StructType schema = new StructType();
            for (int i = 0; i < columns.length; i++) {
                schema = schema.add(columns[i].getName(), convertType(columns[i].getType()), false);
            }
            Dataset<Row> ret = kapSparkSession.read().schema(schema).csv(String.format(CSV_TABLE_DIR, table));
            ret.createOrReplaceTempView(tableDesc.getName());
        }
    }

    private DataType convertType(org.apache.kylin.metadata.datatype.DataType type) {
        if (type.isDateTimeFamily())
            return DataTypes.StringType;

        if (type.isIntegerFamily())
            return DataTypes.LongType;

        if (type.isNumberFamily())
            return DataTypes.createDecimalType(19, 4);

        if (type.isStringFamily())
            return DataTypes.StringType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

    private Map<String, String> fetchPartialQueries(String folder, int start, int end) throws IOException {
        File sqlFile = new File(IT_SQL_BASE_DIR + File.separator + folder);
        Map<String, String> originalSqls = retrieveITSqls(sqlFile);
        Map<String, String> partials = Maps.newLinkedHashMap();

        if (end - start <= 0)
            partials = originalSqls;
        else {
            for (int i = start; i < end; i++) {
                StringBuilder key = new StringBuilder();
                key.append("query");
                if (i < 10)
                    key.append("0");
                key.append(i).append(".sql");
                String sqlContext = originalSqls.get(key.toString());
                if (sqlContext != null)
                    partials.put(key.toString(), sqlContext);
            }
        }
        doFilter(partials);
        return partials;
    }

    private Map<String, String> retrieveAllQueries(String baseDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (baseDir != null) {
            File sqlDirF = new File(baseDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(baseDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.startsWith("sql_");
                    }
                });
            }
        }

        Map<String, String> sqls = Maps.newLinkedHashMap();

        for (File file : sqlFiles) {
            sqls.putAll(retrieveITSqls(file));
        }
        return sqls;
    }

    private Map<String, String> retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null) {
            if (file.exists() && file.listFiles() != null) {
                sqlFiles = file.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".sql");
                    }
                });
            }
        }
        Map<String, String> sqls = Maps.newLinkedHashMap();
        for (int i = 0; i < sqlFiles.length; i++) {
            sqls.put(sqlFiles[i].getName(), FileUtils.readFileToString(sqlFiles[i], "UTF-8"));
        }
        return sqls;
    }

    private void doFilter(Map<String, String> sources) {
        Preconditions.checkArgument(sources != null);
        for (String file : EXCLUDE_SQL_LIST) {
            sources.remove(file);
        }
    }
}
