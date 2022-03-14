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

import io.kyligence.kap.util.ExecAndComp;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.relnode.ContextUtil;

import lombok.val;

public class NQueryPartialMatchIndexTest extends NLocalWithSparkSessionTest {

    private String dfName = "cce7b90d-c1ac-49ef-abc3-f8971eb91544";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/partial_match_index");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "kylin";
    }

    @Test
    public void testQueryPartialMatchIndex() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), "kylin");
        NDataflow df = dsMgr.getDataflow(dfName);
        String sql = "select count(*) from TEST_KYLIN_FACT where cal_dt > '2012-01-01' and cal_dt < '2014-01-01'";

        val expectedRanges = Lists.<Pair<String, String>>newArrayList();
        val segmentRange = Pair.newPair("2012-01-01", "2013-01-01");
        expectedRanges.add(segmentRange);

        QueryContext.current().setPartialMatchIndex(true);
        ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        val context = ContextUtil.listContexts().get(0);
        val segmentIds = context.storageContext.getPrunedSegments();
        assertPrunedSegmentRange(df.getModel().getId(), segmentIds, expectedRanges);
    }

    @Test
    public void testQueryPartialMatchIndexWhenPushdown() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), "kylin");

        NDataflow df = dsMgr.getDataflow(dfName);
        String sql = "select count(*) from TEST_KYLIN_FACT where cal_dt > '2013-01-01' and cal_dt < '2014-01-01'";

        QueryContext.current().setPartialMatchIndex(true);
        try {
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        } catch (Exception e) {
            if (e.getCause() instanceof NoRealizationFoundException) {
                val context = ContextUtil.listContexts().get(0);
                val segmentIds = context.storageContext.getPrunedSegments();
                assertPrunedSegmentRange(df.getModel().getId(), segmentIds, Lists.newArrayList());
            }
        }
    }


    private void assertPrunedSegmentRange(String dfId, List<NDataSegment> prunedSegments,
                                          List<Pair<String, String>> expectedRanges) {
        val model = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(dfId);
        val partitionColDateFormat = model.getPartitionDesc().getPartitionDateFormat();

        if (org.apache.commons.collections.CollectionUtils.isEmpty(expectedRanges)) {
            return;
        }
        Assert.assertEquals(expectedRanges.size(), prunedSegments.size());
        for (int i = 0; i < prunedSegments.size(); i++) {
            val segment = prunedSegments.get(i);
            val start = DateFormat.formatToDateStr(segment.getTSRange().getStart(), partitionColDateFormat);
            val end = DateFormat.formatToDateStr(segment.getTSRange().getEnd(), partitionColDateFormat);
            val expectedRange = expectedRanges.get(i);
            Assert.assertEquals(expectedRange.getFirst(), start);
            Assert.assertEquals(expectedRange.getSecond(), end);
        }
    }
}
