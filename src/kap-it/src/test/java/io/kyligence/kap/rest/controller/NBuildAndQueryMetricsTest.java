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
package io.kyligence.kap.rest.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.source.jdbc.H2Database;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.job.execution.NSparkCubingJob;
import io.kyligence.kap.job.execution.merger.AfterBuildResourceMerger;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import io.kyligence.kap.util.JobFinishHelper;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//TODO need to be rewritten
@Ignore
public class NBuildAndQueryMetricsTest extends AbstractMVCIntegrationTestCase {

    private static final String CSV_TABLE_DIR = TempMetadataBuilder.TEMP_TEST_METADATA + "/data/%s.csv";
    protected static SparkConf sparkConf;
    protected static SparkSession ss;
    @Autowired
    protected UserService userService;
    @Autowired
    QueryService queryService;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @AfterClass
    public static void afterClass() {
        ss.close();
    }

    private static DataType convertType(org.apache.kylin.metadata.datatype.DataType type) {
        if (type.isTimeFamily())
            return DataTypes.TimestampType;

        if (type.isDateTimeFamily())
            return DataTypes.DateType;

        if (type.isIntegerFamily())
            switch (type.getName()) {
                case "tinyint":
                    return DataTypes.ByteType;
                case "smallint":
                    return DataTypes.ShortType;
                case "integer":
                case "int4":
                    return DataTypes.IntegerType;
                default:
                    return DataTypes.LongType;
            }

        if (type.isNumberFamily())
            switch (type.getName()) {
                case "float":
                    return DataTypes.FloatType;
                case "double":
                    return DataTypes.DoubleType;
                default:
                    if (type.getPrecision() == -1 || type.getScale() == -1) {
                        return DataTypes.createDecimalType(19, 4);
                    } else {
                        return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
                    }
            }

        if (type.isStringFamily())
            return DataTypes.StringType;

        if (type.isBoolean())
            return DataTypes.BooleanType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

    @Before
    public void setup() throws Exception {
        setupPushdownEnv();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        ProjectInstance projectInstance = projectManager.getProject(getProject());
        val overrideKylinProps = projectInstance.getOverrideKylinProps();

        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());

        Preconditions.checkArgument(projectInstance != null);

        for (String table : projectInstance.getTables()) {
            if (!"DEFAULT.TEST_KYLIN_FACT".equals(table) && !"DEFAULT.TEST_ACCOUNT".equals(table)) {
                continue;
            }
            TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, getProject()).getTableDesc(table);
            ColumnDesc[] columns = tableDesc.getColumns();
            StructType schema = new StructType();
            for (ColumnDesc column : columns) {
                schema = schema.add(column.getName(), convertType(column.getType()), false);
            }
            Dataset<Row> ret = ss.read().schema(schema).csv(String.format(Locale.ROOT, CSV_TABLE_DIR, table));
            ret.createOrReplaceTempView(tableDesc.getName());
        }

//        val scheduler = NDefaultScheduler.getInstance(getProject());
//        scheduler.init(new JobEngineConfig(kylinConfig));

        ExecutableManager originExecutableManager = ExecutableManager.getInstance(getTestConfig(), getProject());
        ExecutableManager executableManager = Mockito.spy(originExecutableManager);

        val dsMgr = NDataflowManager.getInstance(kylinConfig, getProject());
        // ready dataflow, segment, cuboid layout
        var df = dsMgr.getDataflowByModelAlias("test_cube_01_sum_expr_with_count_distinct_expr");
        // cleanup all segments first
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflowByModelAlias("test_cube_01_sum_expr_with_count_distinct_expr");
        val layouts = df.getIndexPlan().getAllLayouts();
        val round1 = Lists.newArrayList(layouts);
        val segmentRange = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        val toBuildLayouts = Sets.newLinkedHashSet(round1);
        val execMgr = ExecutableManager.getInstance(kylinConfig, getProject());
        // ready dataflow, segment, cuboid layout
        val oneSeg = dsMgr.appendSegment(df, segmentRange);
        val job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), toBuildLayouts, "ADMIN", null);
        // launch the job
        execMgr.addJob(job);
        JobFinishHelper.waitJobFinish(kylinConfig, getProject(), job.getId(), 600 * 1000);
        Preconditions.checkArgument(executableManager.getJob(job.getId()).getStatus() == ExecutableState.SUCCEED);

        val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
        val merger = new AfterBuildResourceMerger(kylinConfig, getProject());
        val layoutIds = toBuildLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), layoutIds, buildStore);

        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexManager.updateIndexPlan("73e06974-e642-6b91-e7a0-5cd7f02ec4f2", copyForWrite -> {
            List<IndexEntity> indexes = copyForWrite.getIndexes().stream().peek(i -> {
                if (i.getId() == 0) {
                    i.setLayouts(Lists.newArrayList(i.getLayouts().get(0)));
                }
            }).collect(Collectors.toList());
            copyForWrite.setIndexes(indexes);
        });
        userService.createUser(new ManagedUser("ADMIN", "KYLIN", false,
                Collections.singletonList(new UserGrantedAuthority("ROLE_ADMIN"))));
    }

    @After
    public void teardown() throws Exception {
        cleanPushdownEnv();
//        NDefaultScheduler.destroyInstance();
    }

    @Test
    public void testMetricsScanForPushDown() throws Exception {
        String sql = "select account_id from test_account limit 30";
        assertMetric(sql, 30, 95556);
    }

    @Test
    public void testMetricsScanForTableIndex() throws Exception {
        String sql = "select count(distinct case when trans_id > 100 then order_id else 0 end),"
                + "sum(case when trans_id > 100 then price else 0 end), price from test_kylin_fact group by price limit 20";
        assertMetric(sql, 10000, 1451142);
    }

    @Test
    public void testMetricsScanForTableIndex2() throws Exception {
        String sql = "select trans_id from test_kylin_fact limit 20";
        assertMetric(sql, 4096, 54888);
    }

    @Test
    public void testMetricsScanForAggIndex() throws Exception {
        String sql = "select trans_id from test_kylin_fact group by trans_id limit 20";
        assertMetric(sql, 10000, 75104);
    }

    private void assertMetric(String sql, long scanRowsExpect, long scanBytesExpect) throws Exception {
        val req = new SQLRequest();
        req.setSql(sql);
        req.setProject(getProject());
        req.setUsername("ADMIN");
        val response = queryService.query(req);
        long scanRows = response.getScanRows().get(0);
        long scanBytes = response.getScanBytes().get(0);
        Assert.assertEquals(scanRowsExpect, scanRows);
        Assert.assertEquals(scanBytesExpect, scanBytes);
    }

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "true");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        overwriteSystemProp("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        overwriteSystemProp("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        overwriteSystemProp("kylin.query.pushdown.jdbc.username", "sa");
        overwriteSystemProp("kylin.query.pushdown.jdbc.password", "");
    }

    private void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        h2Connection.close();
    }

    protected String getProject() {
        return "sum_expr_with_count_distinct";
    }
}
