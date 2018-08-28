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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NCubePlanSelectProposerTest extends NLocalFileMetadataTestCase {
    KylinConfig kylinConfig;
    static final String DEFAULT_PROJECT = "default";
    String[] sqls = { // 
            "select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as GMV \n"
                    + " , count(*) as TRANS_CNT from test_kylin_fact \n"
                    + " where test_kylin_fact.lstg_format_name is not null \n"
                    + " group by test_kylin_fact.lstg_format_name \n" + " having sum(price)>5000 or count(*)>20 " };

    @Before
    public void init() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.storage.provider.0", "io.kyligence.kap.storage.NDataStorage");
        kylinConfig.setProperty("kap.storage.columnar.hdfs-dir", kylinConfig.getHdfsWorkingDirectory() + "/parquet/");
        kylinConfig.setProperty("kap.smart.conf.model.inner-join.exactly-match", "true");
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
    }

    @Test
    public void test() {
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqls);
        smartMaster.analyzeSQLs();

        // validate select the expected model
        smartMaster.selectModel();
        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertEquals("nmodel_basic", mdCtx.getTargetModel().getName());
        Assert.assertEquals("nmodel_basic", mdCtx.getOrigModel().getName());

        // validate select the expected CubePlan
        smartMaster.selectCubePlan();
        Assert.assertEquals("all_fixed_length", mdCtx.getOrigCubePlan().getName());
        Assert.assertEquals("all_fixed_length", mdCtx.getTargetCubePlan().getName());
    }
}
