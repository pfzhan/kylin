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

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import org.junit.Ignore;
import org.junit.Test;

public class NAutoTpchTest extends NAutoTestBase {

    @Override
    public String getProject() {
        return "tpch";
    }

    //KAP#7892 fix this
    @Ignore
    @Test
    public void testTpch() throws Exception {
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
        /*
        * Reason for not using CompareLevel.SAME:
        * See #7257, #7268, #7269
        * Plus the precision difference between query cuboid and SparkSQL
        */
        new TestScenario("sql_tpch", CompareLevel.SAME_ROWCOUNT).execute();
    }

    @Test
    public void testReProposeCase() throws Exception {
        // verify issue https://github.com/Kyligence/KAP/issues/7515
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
        for (int i = 0; i < 2; ++i) {
            new TestScenario("sql_tpch", CompareLevel.SAME, 1, 2).execute();
        }
    }
}
