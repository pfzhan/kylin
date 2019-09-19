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

package io.kyligence.kap.newten.auto;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.NSmartMaster;

public class NReuseModelCcTest extends NAutoTestBase {

    @Override
    public String getProject() {
        /**
         * there are 2 online model in this project, AUTO_MODEL_TEST_KYLIN_FACT_1 and AUTO_MODEL_TEST_KYLIN_FACT_2.
         * fact1 contain CC2 {item_count + price} and fact2 contains CC1( item_count * price )
         *
         */
        return "smart_reuse_existed_models";
    }

    @Test
    public void test_CcReplacedUsedModel_sameWith_reuseModelInModelProposal() {
        String sql = "select count(item_count + price) from test_kylin_fact group by cal_dt";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { sql });
        smartMaster.runAll();

        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sql).isNotSucceed());
        Assert.assertEquals("AUTO_MODEL_TEST_KYLIN_FACT_1",
                smartMaster.getContext().getModelContexts().get(0).getTargetModel().getAlias());
    }
}
