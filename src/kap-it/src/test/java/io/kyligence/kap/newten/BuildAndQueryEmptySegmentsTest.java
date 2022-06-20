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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.execution.NSparkMergingJob;
import io.kyligence.kap.job.execution.merger.AfterMergeOrRefreshResourceMerger;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.util.ExecAndComp;

public class BuildAndQueryEmptySegmentsTest extends NLocalWithSparkSessionTest {

    private static final String DF_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private static final String DF_NAME2 = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";

    private static final String SQL = "select\n" + " count(1) as TRANS_CNT \n" + " from test_kylin_fact \n"
            + " group by trans_id";

    private static final String SQL_DERIVED = "SELECT \n" + "test_cal_dt.season_beg_dt\n"
            + "FROM test_kylin_fact LEFT JOIN edw.test_cal_dt as test_cal_dt \n"
            + "ON test_kylin_fact.cal_dt=test_cal_dt.cal_dt \n"
            + "WHERE test_kylin_fact.cal_dt>'2009-06-01' and test_kylin_fact.cal_dt<'2013-01-01' \n"
            + "GROUP BY test_cal_dt.season_beg_dt";

    private KylinConfig config;
    private NDataflowManager dsMgr;
    private ExecutableManager execMgr;

    @Before
    public void init() throws Exception {
        super.init();
        config = KylinConfig.getInstanceFromEnv();
        dsMgr = NDataflowManager.getInstance(config, getProject());
        execMgr = ExecutableManager.getInstance(config, getProject());
        NIndexPlanManager ipMgr = NIndexPlanManager.getInstance(config, getProject());
        String cubeId = dsMgr.getDataflow(DF_NAME1).getIndexPlan().getUuid();
        IndexPlan cube = ipMgr.getIndexPlan(cubeId);
        Set<Long> tobeRemovedLayouts = cube.getAllLayouts().stream().filter(layout -> layout.getId() != 10001L)
                .map(LayoutEntity::getId).collect(Collectors.toSet());

        cube = ipMgr.updateIndexPlan(dsMgr.getDataflow(DF_NAME1).getIndexPlan().getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(tobeRemovedLayouts, true, true);
        });
        System.out.println(cube.getAllLayouts());
    }

    @After
    public void cleanup() {
        //TODO need to be rewritten
        // NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
    }

    @Test
    public void testEmptySegments() throws Exception {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, getProject());
        dataflowManager.updateDataflowStatus(DF_NAME2, RealizationStatusEnum.OFFLINE);

        cleanupSegments(DF_NAME1);

        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        buildCube(DF_NAME1, SegmentRange.dateToLong("2009-01-01"), SegmentRange.dateToLong("2009-06-01"));
        Assert.assertEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(0).getSegDetails().getTotalRowCount());

        testQueryUnequal(SQL);
        testQueryUnequal(SQL_DERIVED);

        buildCube(DF_NAME1, SegmentRange.dateToLong("2009-06-01"), SegmentRange.dateToLong("2010-01-01"));
        Assert.assertEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(1).getSegDetails().getTotalRowCount());
        buildCube(DF_NAME1, SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2012-01-01"));
        Assert.assertEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(2).getSegDetails().getTotalRowCount());
        buildCube(DF_NAME1, SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2015-01-01"));
        Assert.assertNotEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(3).getSegDetails().getTotalRowCount());

        mergeSegments("2009-01-01", "2010-01-01", true);
        mergeSegments("2010-01-01", "2015-01-01", true);

        testQuery(SQL);
        testQuery(SQL_DERIVED);

        dataflowManager.updateDataflowStatus(DF_NAME2, RealizationStatusEnum.ONLINE);
    }

    private void cleanupSegments(String dfName) {
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    private void buildCube(String dfName, long start, long end) throws Exception {
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);
    }

    private void mergeSegments(String start, String end, boolean force) throws Exception {
        NDataflow df = dsMgr.getDataflow(DF_NAME1);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        NDataSegment emptyMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong(start), SegmentRange.dateToLong(end)), force);
        NSparkMergingJob emptyMergeJob = NSparkMergingJob.merge(emptyMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(emptyMergeJob);
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(emptyMergeJob));
        AfterMergeOrRefreshResourceMerger merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(emptyMergeJob.getSparkMergingStep());
    }

    private void testQuery(String sqlStr) {
        Dataset dsFromCube = ExecAndComp.queryModelWithoutCompute(getProject(), sqlStr);
        Assert.assertNotEquals(0L, dsFromCube.count());
        String sql = convertToSparkSQL(sqlStr);
        Dataset dsFromSpark = ExecAndComp.querySparkSql(sql);
        Assert.assertEquals(dsFromCube.count(), dsFromSpark.count());
    }

    private void testQueryUnequal(String sqlStr) {

        Dataset dsFromCube = ExecAndComp.queryModelWithoutCompute(getProject(), sqlStr);
        if (dsFromCube != null) {
            Assert.assertEquals(0L, dsFromCube.count());
            String sql = convertToSparkSQL(sqlStr);
            Dataset dsFromSpark = ExecAndComp.querySparkSql(sql);
            Assert.assertNotEquals(dsFromCube.count(), dsFromSpark.count());
        }
    }

    private String convertToSparkSQL(String sqlStr) {
        return sqlStr.replaceAll("edw\\.", "");
    }

}
