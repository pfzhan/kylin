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

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.util.ConvertToComputedColumn;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class AutoBuildAndQueryModelViewTest extends AutoTestBase {

    @Test
    public void test() {
        autoGenCCAndCheckModelViewCCQuery(
                "select sum(ITEM_COUNT * PRICE) from test_kylin_fact",
                "select seller_id ,sum(ITEM_COUNT * PRICE), count(1) from {view} group by LSTG_FORMAT_NAME ,seller_id",
                "select seller_id ,sum({cc1}), count(1) from {view} group by LSTG_FORMAT_NAME ,seller_id"
        );
    }

    private void autoGenCCAndCheckModelViewCCQuery(String ccGenSQL, String modelViewSQL, String expectedTransformedModelViewSQL) {
        String[] sqlHasCC_AUTO_1 = new String[]{ ccGenSQL };
        val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), sqlHasCC_AUTO_1);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        NDataModel targetModel = context.getModelContexts().get(0).getTargetModel();
        String targetModelAlias = targetModel.getAlias();
        String sql = modelViewSQL.replace("{view}", "newten." + targetModelAlias);
        String transformed = new ConvertToComputedColumn().transform(sql, getProject(), "default");

        expectedTransformedModelViewSQL = expectedTransformedModelViewSQL.replace("{view}", "newten." + targetModelAlias);
        int idx = 1;
        for (ComputedColumnDesc cc : targetModel.getComputedColumnDescs()) {
            expectedTransformedModelViewSQL = expectedTransformedModelViewSQL.replace("{cc"+idx+"}", targetModelAlias + "." + cc.getColumnName());
            idx++;
        }
        Assert.assertEquals(expectedTransformedModelViewSQL, transformed);
    }
}
