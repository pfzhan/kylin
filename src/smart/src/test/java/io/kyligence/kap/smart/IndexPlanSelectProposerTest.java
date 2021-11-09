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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class IndexPlanSelectProposerTest extends NLocalFileMetadataTestCase {
    private KylinConfig kylinConfig;
    private static final String DEFAULT_PROJECT = "default";
    private String[] sqls = { // 
            "select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as GMV \n"
                    + " , count(*) as TRANS_CNT from test_kylin_fact \n"
                    + " where test_kylin_fact.lstg_format_name is not null \n"
                    + " group by test_kylin_fact.lstg_format_name \n" + " having sum(price)>5000 or count(*)>20 " };

    @Before
    public void init() {
        this.createTestMetadata();
        kylinConfig = getTestConfig();
    }

    @Test
    public void test() {
        String expectedModelId = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        val context = AccelerationContextUtil.newSmartContext(kylinConfig, DEFAULT_PROJECT, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.getProposer("SQLAnalysisProposer").execute();

        // validate select the expected model
        smartMaster.getProposer("ModelSelectProposer").execute();
        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        Assert.assertEquals(expectedModelId, mdCtx.getTargetModel().getUuid());
        Assert.assertEquals(expectedModelId, mdCtx.getOriginModel().getUuid());

        // validate select the expected CubePlan
        smartMaster.getProposer("IndexPlanSelectProposer").execute();
        Assert.assertEquals(expectedModelId, mdCtx.getOriginIndexPlan().getUuid());
        Assert.assertEquals(expectedModelId, mdCtx.getTargetIndexPlan().getUuid());
    }
}
