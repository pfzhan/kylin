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

package io.kyligence.kap.smart;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.query.QueryRecord;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;

@Ignore
public class NSmartQueryMockupTest extends NLocalFileMetadataTestCase {
    private static final String[] SQL_EXTS = { "sql" };
    private static final String PROJ_NAME = "smart";
    private static final String joinType = "left";

    private static final String KAP_QUERY_DIR = "resources/query/";
    private static final String KYLIN_QUERY_DIR = "../../kylin/kylin-it/src/test/resources/query/";

    private MockupQueryExecutor exec = new MockupQueryExecutor();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCommon() throws IOException {
        testRoundTrip(KYLIN_QUERY_DIR + "sql");
    }

    @Test
    public void testSnowflakeQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_snowflake");
    }

    @Test
    public void testDateTimeQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_snowflake");
    }

    @Test
    public void testExtendedColumnQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_extended_column");
    }

    @Test
    public void testLikeQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_like");
    }

    @Test
    public void testVerifyCountQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_verifyCount");
    }

    @Test
    public void testVerifyCountQueryWithPrepare() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_verifyCount");
    }

    @Test
    public void testVerifyContentQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_verifyContent");
    }

    @Test
    public void testOrderByQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_orderby");
    }

    @Test
    public void testLookupQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_lookup");
    }

    @Test
    public void testJoinCastQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_join");
    }

    @Test
    public void testUnionQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_union");
    }

    @Test
    public void testCachedQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_cache");
    }

    @Test
    public void testDerivedColumnQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_derived");
    }

    @Test
    public void testDistinctCountQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_distinct");
    }

    @Test
    public void testTopNQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_topn");
    }

    @Test
    public void testPreciselyDistinctCountQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_distinct_precisely");
    }

    @Test
    public void testIntersectCountQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_intersect_count");
    }

    @Test
    public void testMultiModelQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_multi_model");
    }

    @Test
    public void testDimDistinctCountQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_distinct_dim");
    }

    // FIXME: count(seller_id) not supported in query[18,19,20].sql
    // COPY
    @Test
    public void testTableauQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_tableau");
    }

    @Test
    public void testSubQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_subquery");
    }

    @Test
    public void testCaseWhen() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_casewhen");
    }

    @Test
    public void testHiveQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_hive");
    }

    @Test
    public void testH2Query() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_orderby");
    }

    @Test
    public void testDynamicQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_dynamic");
    }

    @Test
    public void testLimitEnabled() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_limit");
    }

    @Test
    public void testRawQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_raw");
    }

    @Test
    public void testGroupingQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_grouping");
    }

    @Test
    public void testWindowQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_window");
    }

    @Test
    public void testPercentileQuery() throws Exception {
        testRoundTrip(KYLIN_QUERY_DIR + "sql_percentile");
    }

    private void testRoundTrip(String sqlDirectory) throws IOException {
        String[] sqls = readSQLs(sqlDirectory);

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJ_NAME, sqls);
        smartMaster.runAll();

        fillTestDataflow();

        // Sleep 0.5s to ensure all changes take effect
        try {
            Thread.sleep(500);
        } catch (Exception e) {
            // do not catch
        }

        for (String sql : sqls)
            verifySQLs(sql);
    }

    private String[] readSQLs(String sqlDirectory) throws IOException {
        File sqlDir = new File(sqlDirectory);
        Preconditions.checkArgument(sqlDir.exists(), "SQL Directory not found.");
        Collection<File> sqlFiles = FileUtils.listFiles(sqlDir, SQL_EXTS, true);
        if (CollectionUtils.isEmpty(sqlFiles))
            return new String[0];

        String[] sqls = new String[sqlFiles.size()];
        int i = 0;
        for (File sqlFile : sqlFiles) {
            String sql = "-- " + sqlFile.getName() + "\n" + FileUtils.readFileToString(sqlFile);
            sqls[i++] = changeJoinType(sql, joinType);
        }
        return sqls;
    }

    private void fillTestDataflow() throws IOException {
        NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), PROJ_NAME);
        NProjectManager pm = NProjectManager.getInstance(getTestConfig());
        for (IRealization real : pm.listAllRealizations(PROJ_NAME)) {
            if (real instanceof NDataflow) {
                NDataflow copy = dfMgr.getDataflow(real.getName()).copy();
                NDataSegment segment = new NDataSegment();
                segment.setId(0);
                segment.setName("TEST");
                segment.setDataflow(copy);
                segment.setStatus(SegmentStatusEnum.READY);
                segment.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));

                NDataflowUpdate update = new NDataflowUpdate(copy.getName());
                update.setToAddSegs(segment);
                update.setStatus(RealizationStatusEnum.READY);
                List<NDataCuboid> cuboids = Lists.newArrayList();
                for (NCuboidDesc cuboidDesc : copy.getCubePlan().getCuboids()) {
                    for (NCuboidLayout layout : cuboidDesc.getLayouts()) {
                        NDataCuboid c = NDataCuboid.newDataCuboid(copy, segment.getId(), layout.getId());
                        c.setStatus(SegmentStatusEnum.READY);
                        cuboids.add(c);
                    }
                }
                update.setToAddOrUpdateCuboids(cuboids.toArray(new NDataCuboid[0]));
                dfMgr.updateDataflow(update);
            }
        }
    }

    private void verifySQLs(String sql) {
        QueryRecord qr = exec.execute(PROJ_NAME, sql);
        Collection<OLAPContext> ctxs = qr.getOLAPContexts();
        if (CollectionUtils.isEmpty(ctxs)) {
            Assert.assertEquals(SQLResult.Status.SUCCESS, qr.getSqlResult().getStatus());
        } else {
            Assert.assertFalse(ctxs.isEmpty());
            for (OLAPContext ctx : ctxs) {
                if (ctx.firstTableScan == null)
                    continue;

                if (ctx.enumeratorType == null)
                    System.out.println("");
                switch (ctx.enumeratorType) {
                case OLAP:
                    Assert.assertNotNull(ctx.realization);
                    Assert.assertNotNull(ctx.storageContext.getCuboidId());
                    Assert.assertTrue(ctx.realization instanceof NDataflow);

                    NDataflow df = (NDataflow) ctx.realization;
                    Assert.assertNotNull(df.getLastSegment().getCuboid(ctx.storageContext.getCuboidId()));
                    break;
                default:
                    break;
                }
            }
        }
        // more assertion here
        System.out.println(qr);

    }

    private String changeJoinType(String sql, String targetType) {
        if (targetType.equalsIgnoreCase("default"))
            return sql;

        String specialStr = "changeJoinType_DELIMITERS";
        sql = sql.replaceAll(System.getProperty("line.separator"), " " + specialStr + " ");

        String[] tokens = StringUtils.split(sql, null);// split white spaces
        for (int i = 0; i < tokens.length - 1; ++i) {
            if ((tokens[i].equalsIgnoreCase("inner") || tokens[i].equalsIgnoreCase("left"))
                    && tokens[i + 1].equalsIgnoreCase("join")) {
                tokens[i] = targetType.toLowerCase();
            }
        }

        String ret = StringUtils.join(tokens, " ");
        ret = ret.replaceAll(specialStr, System.getProperty("line.separator"));

        return ret;
    }
}
