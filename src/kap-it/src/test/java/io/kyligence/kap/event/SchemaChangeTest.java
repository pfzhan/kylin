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
package io.kyligence.kap.event;

import static org.awaitility.Awaitility.with;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
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
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
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
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.service.TableService;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import io.kyligence.kap.util.JobFinishHelper;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Ignore("disable unstable test")
@Slf4j
public class SchemaChangeTest extends AbstractMVCIntegrationTestCase {

    private static final String SQL_LOOKUP = "select cal_dt, week_beg_dt from edw.test_cal_dt";
    private static final String SQL_DERIVED = "select test_sites.site_name, test_kylin_fact.lstg_format_name, sum(test_kylin_fact.price) as gmv, count(*) as trans_cnt \n"
            + " from test_kylin_fact left join edw.test_cal_dt as test_cal_dt\n"
            + " on test_kylin_fact.cal_dt = test_cal_dt.cal_dt  left join test_category_groupings\n"
            + " on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id and test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
            + " left join edw.test_sites as test_sites  on test_kylin_fact.lstg_site_id = test_sites.site_id\n"
            + " group by   test_sites.site_name, test_kylin_fact.lstg_format_name";
    private static final String SQL_LOOKUP2 = "select categ_lvl3_name, categ_lvl2_name, site_id, meta_categ_name, leaf_categ_id  from test_category_groupings";
    private static final String SQL_DERIVED2 = "select upd_user,count(1) as cnt\n"
            + "from test_kylin_fact as test_kylin_fact\n"
            + "left join test_category_groupings as test_category_groupings\n"
            + "on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id and test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
            + "where upd_user not in ('user_y') group by upd_user";

    private static final String TABLE_IDENTITY = "DEFAULT.TEST_CATEGORY_GROUPINGS";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @Autowired
    TableService tableService;

    @Autowired
    QueryService queryService;

    @Autowired
    protected UserService userService;

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
        JobContextUtil.cleanUp();
    }

    @Before
    public void setup() throws Exception {
        setupPushdownEnv();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(getProject());
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());

        ExecutableManager originExecutableManager = ExecutableManager.getInstance(getTestConfig(), getProject());
        ExecutableManager executableManager = Mockito.spy(originExecutableManager);

        val config = KylinConfig.getInstanceFromEnv();
        val dsMgr = NDataflowManager.getInstance(config, getProject());
        // ready dataflow, segment, cuboid layout
        var df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        // cleanup all segments first
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegsWithArray(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        val layouts = df.getIndexPlan().getAllLayouts();
        val round1 = Lists.newArrayList(layouts);
        val segmentRange = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        val toBuildLayouts = Sets.newLinkedHashSet(round1);
        val execMgr = ExecutableManager.getInstance(config, getProject());
        // ready dataflow, segment, cuboid layout
        val oneSeg = dsMgr.appendSegment(df, segmentRange);
        val job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), toBuildLayouts, "ADMIN", null);
        // launch the job
        execMgr.addJob(job);
        JobFinishHelper.waitJobFinish(config, getProject(), job.getId(), 600 * 1000);
        Preconditions.checkArgument(executableManager.getJob(job.getId()).getStatus() == ExecutableState.SUCCEED);

        val buildStore = ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep());
        val merger = new AfterBuildResourceMerger(config, getProject());
        val layoutIds = toBuildLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), layoutIds, buildStore);

        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexManager.updateIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", copyForWrite -> {
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
        JobContextUtil.cleanUp();
    }

    @Test
    public void testSnapshotModifyTimeAfterReloadTable() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val table = tableManager.getTableDesc(TABLE_IDENTITY);
        long snapshotLastModify = System.currentTimeMillis();
        table.setLastSnapshotPath("mockpath");
        table.setSnapshotLastModified(snapshotLastModify);
        tableManager.saveSourceTable(table);
        tableService.reloadTable(getProject(), TABLE_IDENTITY, false, -1, true);
        val newTable = tableManager.getTableDesc(TABLE_IDENTITY);
        Assert.assertEquals(snapshotLastModify, newTable.getSnapshotLastModified());
    }

    @Test
    public void testAddColumn() throws Exception {
        addColumn(TABLE_IDENTITY, new ColumnDesc("", "tmp1", "bigint", "", "", "", null));
        tableService.reloadTable(getProject(), TABLE_IDENTITY, false, -1, true);
        assertSqls();
    }

    @Test
    public void testRemoveColumn() throws Exception {
        removeColumn(TABLE_IDENTITY, "SRC_ID");
        tableService.reloadTable(getProject(), TABLE_IDENTITY, false, -1, true);
        assertSqls();
    }

    @Ignore
    @Test
    public void testChangeColumnType() throws Exception {
        changeColumns(TABLE_IDENTITY, Sets.newHashSet("SRC_ID"), columnDesc -> columnDesc.setDatatype("string"));
        tableService.reloadTable(getProject(), TABLE_IDENTITY, false, -1, true);
        assertSqls();
    }

    @Test
    public void testChangeColumnOrder() throws Exception {
        changeColumns(TABLE_IDENTITY, Sets.newHashSet("SRC_ID", "GCS_ID"), columnDesc -> {
            if ("SRC_ID".equals(columnDesc.getName())) {
                columnDesc.setId("32");
            } else {
                columnDesc.setId("35");
            }
        });
        Pair<String, List<String>> pair = tableService.reloadTable(getProject(), TABLE_IDENTITY, false, -1, true);
        //don't need to reload
        Assert.assertEquals(0, pair.getSecond().size());
    }

    private void assertSqls() throws Exception {
        for (Pair<String, Boolean> pair : Arrays.asList(Pair.newPair(SQL_LOOKUP, false),
                Pair.newPair(SQL_DERIVED, false), Pair.newPair(SQL_LOOKUP2, true), Pair.newPair(SQL_DERIVED2, true))) {
            val req = new SQLRequest();
            req.setSql(pair.getFirst());
            req.setProject(getProject());
            req.setUsername("ADMIN");
            val response = queryService.query(req);
            with().pollInterval(10, TimeUnit.MILLISECONDS) //
                    .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                    .await().atMost(100000, TimeUnit.MILLISECONDS) //
                    .untilAsserted(() -> {
                        String message = pair.getFirst() + " failed";
                        Assert.assertEquals(message, pair.getSecond(), response.isQueryPushDown());
                    });
        }
    }

    private void changeColumns(String tableIdentity, Set<String> columns, Consumer<ColumnDesc> changer)
            throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Stream.of(tableManager.copyForWrite(factTable).getColumns()).peek(col -> {
            if (columns.contains(col.getName())) {
                changer.accept(col);
            }
        }).sorted(Comparator.comparing(col -> Integer.parseInt(col.getId()))).toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(tablePath), tableMeta);
    }

    private void addColumn(String tableIdentity, ColumnDesc... columns) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Lists.newArrayList(factTable.getColumns());
        long maxId = newColumns.stream().mapToLong(col -> Long.parseLong(col.getId())).max().orElse(0);
        for (ColumnDesc column : columns) {
            maxId++;
            column.setId("" + maxId);
            newColumns.add(column);
        }
        tableMeta.setColumns(newColumns.toArray(new ColumnDesc[0]));
        JsonUtil.writeValueIndent(new FileOutputStream(tablePath), tableMeta);
    }

    private void removeColumn(String tableIdentity, String... column) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val columns = Sets.newHashSet(column);
        val newColumns = Stream.of(factTable.getColumns()).filter(col -> !columns.contains(col.getName()))
                .toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(tablePath), tableMeta);
    }

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
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
        return "default";
    }
}
