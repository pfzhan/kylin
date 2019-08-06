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

import org.apache.kylin.common.KylinConfig;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;

public class NAutoTdvtTest extends NAutoTestBase {

    @Override
    public String getProject() {
        return "tdvt";
    }

    @Test
    public void testTdvt() throws Exception {
        new TestScenario(CompareLevel.NONE, "sql_tdvt").execute();
    }

    @Test
    public void testSameLevelOfTdvt() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql_tdvt/same_level").execute();
    }

    @Ignore("for testing")
    @Test
    public void test() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("calcite.debug", "true");
        new TestScenario(CompareLevel.SAME, "query/temp").execute();
    }
}
