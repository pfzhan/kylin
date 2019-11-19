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

package io.kyligence.kap.smart.model;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartMaster;

import java.lang.reflect.Field;

public class NQueryScopeProposerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testTransferToNamedColumn() throws Exception {
        final String sql = "select order_id from TEST_KYLIN_FACT";
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), getProject(), new String[] { sql });
        smartMaster.runAll();
        NQueryScopeProposer nQueryScopeProposer = new NQueryScopeProposer(
                smartMaster.getContext().getModelContexts().get(0));
        NQueryScopeProposer.ScopeBuilder scopeBuilder = nQueryScopeProposer.new ScopeBuilder(
                smartMaster.getRecommendedModels().get(0));

        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.A_B"), 1, "C", "double");
        Field f1 = col1.getClass().getDeclaredField("identity");
        f1.setAccessible(true);
        f1.set(col1, "A_B.C");
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.A"), 2, "B_C", "double");
        Field f2 = col2.getClass().getDeclaredField("identity");
        f2.setAccessible(true);
        f2.set(col2, "A.B_C");

        NDataModel.NamedColumn namedColumn1 = scopeBuilder.transferToNamedColumn(col1, null);
        NDataModel.NamedColumn namedColumn2 = scopeBuilder.transferToNamedColumn(col2, null);
        Assert.assertNotEquals(namedColumn1, namedColumn2);
        Assert.assertEquals("A_B_0_DOT_0_C", namedColumn1.getName());
        Assert.assertEquals("A_0_DOT_0_B_C", namedColumn2.getName());
    }
}
