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

import com.google.common.collect.Lists;
import org.junit.Test;

import io.kyligence.kap.util.ExecAndComp.CompareLevel;

public class AutoBuildAndQuerySSBTest extends AutoTestBase {
    @Test
    public void testDifferentJoinOrder() throws Exception {
        final String TEST_FOLDER = "query/sql_joinorder";
        proposeWithSmartMaster(getProject(), Lists.newArrayList(new TestScenario(CompareLevel.NONE, TEST_FOLDER, 0, 1)));
        TestScenario testQueries = new TestScenario(CompareLevel.SAME, TEST_FOLDER, 1, 5);
        collectQueries(Lists.newArrayList(testQueries));
        buildAndCompare(testQueries);
    }

    @Override
    public String getProject() {
        return "ssb";
    }

}
