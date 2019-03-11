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
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import lombok.val;

public class NTopNTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Override
    public String getProject() {
        return "top_n";
    }

    @Test
    public void testTopNCanNotAnswerNonTopNStyleQuery() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        dfMgr.updateDataflow("fb6ce800-43ee-4ef9-b100-39d523f36304", copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.OFFLINE);
        });
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        fullBuildCube("79547ec2-350e-4ba4-88f9-099048962ceb", getProject());

        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_answer",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) desc limit 1"));
        // TopN will answer TopN style query.
        NExecAndComp.execAndCompare(query, getProject(), CompareLevel.NONE, "left");
        try {
            query.clear();
            query.add(Pair.newPair("can_not_answer", "select sum(PRICE) from TEST_TOP_N group by SELLER_ID"));
            // TopN will not answer sum.
            NExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getCause().getMessage().contains("No realization found for OLAPContext"));
        }
    }

    @Test
    public void testPreferSumMeasure() throws Exception {
        TopNCounter.EXTRA_SPACE_RATE = 1;
        SparkContext existingCxt = SparkContext.getOrCreate(sparkConf);
        existingCxt.stop();
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        ss.sparkContext().setLogLevel("ERROR");
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        fullBuildCube("79547ec2-350e-4ba4-88f9-099048962ceb", getProject());
        fullBuildCube("fb6ce800-43ee-4ef9-b100-39d523f36304", getProject());

        ss.close();

        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        List<Pair<String, String>> query = new ArrayList<>();

        val dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        // let Sum measure answer TOP_N query, it is accurate.
        dfMgr.updateDataflow("79547ec2-350e-4ba4-88f9-099048962ceb", copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.OFFLINE);
        });

        query.add(Pair.newPair("top_n_answer",
                "select sum(PRICE) from TEST_TOP_N group by SELLER_ID order by sum(PRICE) desc limit 1"));
        NExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");

        // let TopN measure answer TOP_N query, it is inaccurate. So the compare will fail
        dfMgr.updateDataflow("79547ec2-350e-4ba4-88f9-099048962ceb", copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.ONLINE);
        });
        dfMgr.updateDataflow("fb6ce800-43ee-4ef9-b100-39d523f36304", copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.OFFLINE);
        });

        try {
            NExecAndComp.execAndCompare(query, getProject(), CompareLevel.SAME, "left");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("result not match"));
        }

    }
}
