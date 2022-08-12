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
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;

public class NComputedColumnTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/comput_column");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "comput_column";
    }

    @Test
    public void testConstantComputeColumn() throws Exception {
        String dfID = "4a45dc4d-937e-43cc-8faa-34d59d4e11d3";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfID);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(layouts),
                true);
        String sqlHitCube = "select (1+2) as c1,(LINEORDER.LO_TAX +1) as c2,(CUSTOMER.C_NAME +'USA') as c3 "
                + "from SSB.P_LINEORDER as LINEORDER join SSB.CUSTOMER as CUSTOMER on LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY "
                + "group by (1+2),(LINEORDER.LO_TAX +1),(CUSTOMER.C_NAME +'USA') ";
        List<String> hitCubeResult = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube).collectAsList().stream()
                .map(Row::toString).collect(Collectors.toList());
        Assert.assertEquals(9, hitCubeResult.size());
    }
}
