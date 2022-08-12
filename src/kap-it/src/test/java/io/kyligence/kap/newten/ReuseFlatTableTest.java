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

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.util.ExecAndComp;

public class ReuseFlatTableTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/reuse_flattable");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());

        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "reuse_flattable";
    }

    @Test
    public void testReuseFlatTable() throws Exception {
        String dfID = "75080248-367e-4bac-9fd7-322517ee0227";
        fullBuild(dfID);
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataflow dataflow = dfManager.getDataflow(dfID);
        NDataSegment firstSegment = dataflow.getFirstSegment();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        indexPlanManager.updateIndexPlan(dfID, copyForWrite -> {
            IndexEntity indexEntity = new IndexEntity();
            indexEntity.setId(200000);
            indexEntity.setDimensions(Lists.newArrayList(2));
            indexEntity.setMeasures(Lists.newArrayList(100000, 100001));
            LayoutEntity layout = new LayoutEntity();
            layout.setId(200001);
            layout.setColOrder(Lists.newArrayList(2, 100000, 100001));
            layout.setIndex(indexEntity);
            layout.setAuto(true);
            layout.setUpdateTime(0);
            indexEntity.setLayouts(Lists.newArrayList(layout));
            copyForWrite.setIndexes(Lists.newArrayList(indexEntity));
        });
        indexDataConstructor.buildSegment(dfID, firstSegment,
                Sets.newLinkedHashSet(dfManager.getDataflow(dfID).getIndexPlan().getAllLayouts()), true, null);
        String query = "select count(distinct trans_id) from TEST_KYLIN_FACT";
        long result = ExecAndComp.queryModel(getProject(), query).collectAsList().get(0).getLong(0);
        long expect = ss.sql("select count(distinct trans_id) from TEST_KYLIN_FACT").collectAsList().get(0).getLong(0);
        Assert.assertEquals(result, expect);
    }
}
