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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import lombok.val;

public class NMatchingTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "match";
    }

    @Test
    public void testCanNotAnswer() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        dfMgr.updateDataflowStatus("073198da-ce0e-4a0c-af38-cc27ae31cc0e", RealizationStatusEnum.OFFLINE);
        fullBuild("83ade475-5b80-483a-ae4b-1144e4f04e81");

        try {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

            List<Pair<String, String>> query = new ArrayList<>();
            query.add(
                    Pair.newPair("can_not_answer", "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
            ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getCause().getCause().getMessage().contains("No realization found for OLAPContext"));
        }

    }

    @Test
    public void testCanAnswer() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        fullBuild("83ade475-5b80-483a-ae4b-1144e4f04e81");
        fullBuild("073198da-ce0e-4a0c-af38-cc27ae31cc0e");

        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_not_answer", "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        ExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
    }
}
