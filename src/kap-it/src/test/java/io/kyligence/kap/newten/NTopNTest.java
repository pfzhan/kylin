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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.metadata.model.SegmentRange.TimePartitionedSegmentRange;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.guava20.shaded.common.base.Throwables;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.cuboid.NQueryLayoutChooser;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.smart.SmartContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import lombok.val;

public class NTopNTest extends NLocalWithSparkSessionTest {

    private NDataflowManager dfMgr = null;

    @Before
    public void setup() {
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");

        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());

        getTestConfig().setProperty("kylin.query.heterogeneous-segment-enabled", "false");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "top_n";
    }

    @Test
    public void testTopNWithMultiDims() throws Exception {
        String dfID = "79547ec2-350e-4ba4-88f9-099048962ceb";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dataflow.getIndexPlan().getLayoutEntity(101001L)), true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,TRANS_ID order by sum(PRICE) desc limit 1";
        String sql2 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) desc limit 1";
        query.add(Pair.newPair("topn_with_multi_dim", sql1));
        query.add(Pair.newPair("topn_with_one_dim", sql2));
        // TopN will answer TopN style query.
        verifyTopnResult(query, dfMgr.getDataflow(dfID));
        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.NONE, "left");
    }

    @Test
    public void testTopNCannotAnswerAscendingTopnQuery() throws Exception {
        String dfID = "79547ec2-350e-4ba4-88f9-099048962ceb";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dataflow.getIndexPlan().getLayoutEntity(101001L)), true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,TRANS_ID order by sum(PRICE) limit 1";
        String sql2 = "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) limit 1";
        query.add(Pair.newPair("topn_with_multi_dim", sql1));
        query.add(Pair.newPair("topn_with_one_dim", sql2));

        try {
            ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage().contains("No realization found for OLAPContext"));
        }
    }

    @Test
    public void testTopNCanNotAnswerNonTopNStyleQuery() throws Exception {
        dfMgr.updateDataflowStatus("fb6ce800-43ee-4ef9-b100-39d523f36304", RealizationStatusEnum.OFFLINE);
        dfMgr.updateDataflowStatus("da101c43-6d22-48ce-88d2-bf0ce0594022", RealizationStatusEnum.OFFLINE);
        String dfID = "79547ec2-350e-4ba4-88f9-099048962ceb";
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dfMgr.getDataflow(dfID).getIndexPlan().getLayoutEntity(100001L),
                        dfMgr.getDataflow(dfID).getIndexPlan().getLayoutEntity(100003L)),
                true);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_answer_single_dim",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) desc limit 1"));
        query.add(Pair.newPair("can_answer_multi_dim",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,TRANS_ID order by sum(PRICE) desc limit 1"));
        // TopN will answer TopN style query.
        verifyTopnResult(query, dfMgr.getDataflow(dfID));
        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.NONE, "left");
        try {
            query.clear();
            query.add(Pair.newPair("can_not_answer", "select sum(PRICE) from TEST_TOP_N group by SELLER_ID"));
            // TopN will not answer sum.
            ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage().contains("No realization found for OLAPContext"));
        }
    }

    @Test
    public void testSingleDimLayoutCannotAnswerMultiTopnQuery() throws Exception {
        dfMgr.updateDataflowStatus("79547ec2-350e-4ba4-88f9-099048962ceb", RealizationStatusEnum.OFFLINE);
        dfMgr.updateDataflowStatus("da101c43-6d22-48ce-88d2-bf0ce0594022", RealizationStatusEnum.OFFLINE);
        String dfID = "fb6ce800-43ee-4ef9-b100-39d523f36304";
        //  layout[ID, count(*), sum(price), Topn(price, SELLER_ID)]
        indexDataConstructor.buildIndex(dfID, TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dfMgr.getDataflow(dfID).getIndexPlan().getLayoutEntity(1L)), true);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("cannot_answer_multi_dim_in_single_dim_index",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID,ID order by sum(PRICE) desc limit 1"));
        try {
            ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage().contains("No realization found for OLAPContext"));
        }
    }

    @Test
    public void testSameTableNameInDifferentDatabase() throws Exception {
        TopNCounter.EXTRA_SPACE_RATE = 1;

        fullBuild("da101c43-6d22-48ce-88d2-bf0ce0594022");

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val sql = "select A.SELLER_ID,sum(A.PRICE) from ISSUES.TEST_TOP_N A "
                + " join TEST_TOP_N B on A.ID=B.ID group by A.SELLER_ID order by sum(B.PRICE) desc limit 100";
        try {
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e.getCause()).getMessage().contains("No realization found for OLAPContext"));
        }

    }

    private void verifyTopnResult(List<Pair<String, String>> queries, NDataflow dataflow) {
        //verify topN measure will answer the multi-Dimension query
        for (Pair<String, String> nameAndQueryPair : queries) {
            OLAPContext context = getOlapContext(nameAndQueryPair.getSecond()).get(0);
            Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
            context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
            val pair = NQueryLayoutChooser.selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(),
                    context.getSQLDigest());
            Assert.assertNotNull(pair);
            Assert.assertEquals(1, pair.getCapabilityResult().influences.size());
            Assert.assertFalse(
                    pair.getCapabilityResult().influences.get(0) instanceof CapabilityResult.DimensionAsMeasure);
            Assert.assertEquals(context.allColumns, Sets.newHashSet(
                    pair.getCapabilityResult().influences.get(0).getInvolvedMeasure().getFunction().getColRefs()));
        }
    }

    private List<OLAPContext> getOlapContext(String sql) {
        val context = new SmartContext(KylinConfig.getInstanceFromEnv(), getProject(), new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();
        List<OLAPContext> ctxs = Lists.newArrayList();
        smartMaster.getContext().getModelContexts()
                .forEach(nModelContext -> ctxs.addAll(nModelContext.getModelTree().getOlapContexts()));
        return ctxs;
    }
}
