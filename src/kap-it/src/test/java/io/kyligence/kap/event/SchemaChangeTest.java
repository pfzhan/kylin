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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLRequest;
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
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.KapQueryService;
import io.kyligence.kap.rest.service.TableService;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import lombok.val;
import lombok.var;

public class SchemaChangeTest extends AbstractMVCIntegrationTestCase {

    private static final String PROJECT = "default";

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

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @Autowired
    TableService tableService;

    @Autowired
    KapQueryService queryService;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }

    @AfterClass
    public static void afterClass() {
        if (Shell.MAC)
            System.clearProperty("org.xerial.snappy.lib.name");//reset

        ss.close();
    }

    @Before
    public void setup() throws Exception {
        setupPushdownEnv();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");

        val scheduler = NDefaultScheduler.getInstance(PROJECT);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());

        val config = KylinConfig.getInstanceFromEnv();
        val dsMgr = NDataflowManager.getInstance(config, PROJECT);
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
        val execMgr = NExecutableManager.getInstance(config, PROJECT);
        // ready dataflow, segment, cuboid layout
        val oneSeg = dsMgr.appendSegment(df, segmentRange);
        val job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), toBuildLayouts, "ADMIN");
        // launch the job
        execMgr.addJob(job);
        if (!Objects.equals(waitForFinished(job), ExecutableState.SUCCEED))
            throw new IllegalStateException();

        val buildStore = ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep());
        val merger = new AfterBuildResourceMerger(config, PROJECT);
        val layoutIds = toBuildLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), layoutIds, buildStore);

        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.updateIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream().peek(i -> {
                if (i.getId() == 0) {
                    i.setLayouts(Lists.newArrayList(i.getLayouts().get(0)));
                }
            }).collect(Collectors.toList()));
        });
    }

    @After
    public void teardown() throws Exception {
        cleanPushdownEnv();
        NDefaultScheduler.destroyInstance();
    }

    private static final String TABLE_IDENTITY = "DEFAULT.TEST_CATEGORY_GROUPINGS";

    @Test
    public void testAddColumn() throws Exception {
        addColumn(TABLE_IDENTITY, new ColumnDesc("", "tmp1", "bigint", "", "", "", null));
        tableService.reloadTable(PROJECT, TABLE_IDENTITY, false, -1);
        assertSqls();
    }

    @Test
    public void testRemoveColumn() throws Exception {
        removeColumn(TABLE_IDENTITY, "SRC_ID");
        tableService.reloadTable(PROJECT, TABLE_IDENTITY, false, -1);
        assertSqls();
    }

    @Test
    public void testChangeColumnType() throws Exception {
        changeTypeColumn(TABLE_IDENTITY, new HashMap<String, String>() {
            {
                put("SRC_ID", "string");
            }
        });
        tableService.reloadTable(PROJECT, TABLE_IDENTITY, false, -1);
        assertSqls();
    }

    private void assertSqls() throws Exception {
        for (Pair<String, Boolean> pair : Arrays.asList(Pair.newPair(SQL_LOOKUP, false),
                Pair.newPair(SQL_DERIVED, false), Pair.newPair(SQL_LOOKUP2, true), Pair.newPair(SQL_DERIVED2, true))) {
            val req = new SQLRequest();
            req.setSql(pair.getFirst());
            req.setProject(PROJECT);
            req.setUsername("ADMIN");
            val response = queryService.query(req);

            Assert.assertEquals(pair.getFirst() + " failed", pair.getSecond(), response.isQueryPushDown());

        }
    }

    private void changeTypeColumn(String tableIdentity, Map<String, String> columns) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Stream.of(tableManager.copyForWrite(factTable).getColumns()).peek(col -> {
            if (columns.containsKey(col.getName())) {
                col.setDatatype(columns.get(col.getName()));
            }
        }).toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void addColumn(String tableIdentity, ColumnDesc... columns) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Lists.newArrayList(factTable.getColumns());
        long maxId = newColumns.stream().mapToLong(col -> Long.parseLong(col.getId())).max().getAsLong();
        for (ColumnDesc column : columns) {
            maxId++;
            column.setId("" + maxId);
            newColumns.add(column);
        }
        tableMeta.setColumns(newColumns.toArray(new ColumnDesc[0]));
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void removeColumn(String tableIdentity, String... column) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val columns = Sets.newHashSet(column);
        val newColumns = Stream.of(factTable.getColumns()).filter(col -> !columns.contains(col.getName()))
                .toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    ExecutableState waitForFinished(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            val status = job.getStatus();
            if (!status.isReadyOrRunning()) {
                return status;
            }
        }
    }

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");
    }

    private void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }
}
