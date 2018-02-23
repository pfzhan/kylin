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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.query.routing.Candidate;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;
import io.kyligence.kap.spark.KapSparkSession;

public class NITQueryTest extends NLocalSparkWithCSVDataTest {
    private KylinConfig kylinConfig;
    private KapSparkSession kapSparkSession;
    private static final String CSV_TABLE_DIR = "../examples/test_metadata/data/%s.csv";
    private static final String DEFAULT_PROJECT = "newten";
    private static final String IT_SQL_BASE_DIR = "../../kylin/kylin-it/src/test/resources/query";
    private static final String[] EXCLUDE_SQL_LIST = { "select sum(price) as GMV" };

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        super.setUp();
        DefaultScheduler scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.storage.provider.0", "io.kyligence.kap.storage.NDataStorage");
    }

    @After
    public void after() throws Exception {
        Candidate.restorePriorities();

        if (kapSparkSession != null)
            kapSparkSession.close();

        DefaultScheduler.destroyInstance();
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
        //String[] queries = retrieveAllQueries(IT_SQL_BASE_DIR);
        String[] queries = fetchPartialQueries("sql", 0, 38);
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        for (String query : queries) {
            kapSparkSession.collectQueries(query);
        }
        kapSparkSession.speedUp();

        // Step2. Query cube and query SparkSQL respectively
        kapSparkSession.close();
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);

        // Query from Cube
        List<Dataset> resultsOfCube = Lists.newArrayList();
        for (String query : queries) {
            Dataset<Row> ret = kapSparkSession.queryFromCube(query);
            resultsOfCube.add(ret);
        }

        // Query from SparkSQL
        prepareBeforeSparkSql();
        List<Dataset> resultsOfSparkSql = Lists.newArrayList();
        for (String sql : queries) {
            // Table schema comes from csv and DATABASE.TABLE is not supported.
            String sqlForSpark = sql.replaceAll("edw.", "").replaceAll("default.", "");
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
            return DataTypes.DateType;

        if (type.isIntegerFamily())
            return DataTypes.LongType;

        if (type.isNumberFamily())
            return DataTypes.createDecimalType(19, 4);

        if (type.isStringFamily())
            return DataTypes.StringType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

    private String[] fetchPartialQueries(String folder, int start, int end) throws IOException {
        File sqlFile = new File(IT_SQL_BASE_DIR + File.separator + folder);
        String[] originalSqls = retrieveITSqls(sqlFile);
        if (start >= 0 && end < originalSqls.length && end - start > 0) {
            String[] newSqls = new String[end - start];
            for (int i = 0; i < originalSqls.length; i++) {
                if (i >= start && i < end)
                    newSqls[i - start] = originalSqls[i];
            }
            originalSqls = newSqls;
        }
        return doFilter(originalSqls);
    }

    private String[] retrieveAllQueries(String baseDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (baseDir != null) {
            File sqlDirF = new File(baseDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(baseDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.startsWith("sql_")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }
        List<String> allSqls = new ArrayList<>();
        for (File file : sqlFiles) {
            allSqls.addAll(Arrays.asList(retrieveITSqls(file)));
        }
        return allSqls.toArray(new String[0]);
    }

    private String[] retrieveITSqls(File file) throws IOException {
        File[] sqlFiles = new File[0];
        if (file != null) {
            if (file.exists() && file.listFiles() != null) {
                sqlFiles = file.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".sql")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }
        String[] sqls = new String[sqlFiles.length];
        for (int i = 0; i < sqlFiles.length; i++) {
            sqls[i] = FileUtils.readFileToString(sqlFiles[i], "UTF-8");
        }
        return sqls;
    }

    private String[] doFilter(String[] sources) {
        Preconditions.checkArgument(sources != null);
        return Collections2.filter(Arrays.asList(sources), new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                for (String sql : EXCLUDE_SQL_LIST) {
                    if (input.contains(sql))
                        return false;
                }
                return true;

            }
        }).toArray(new String[0]);
    }
}
