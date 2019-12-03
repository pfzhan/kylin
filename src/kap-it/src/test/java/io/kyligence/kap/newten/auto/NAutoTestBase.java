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

import java.util.Map;

import org.junit.After;
import org.junit.Before;

import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import io.kyligence.kap.utils.RecAndQueryCompareUtil.CompareEntity;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NAutoTestBase extends NSuggestTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected Map<String, CompareEntity> executeTestScenario(boolean recordFQ,
            NSuggestTestBase.TestScenario... testScenarios) throws Exception {

        // 1. execute auto-modeling propose
        long startTime = System.currentTimeMillis();
        final NSmartMaster smartMaster = proposeWithSmartMaster(getProject(), testScenarios);
        final Map<String, CompareEntity> compareMap = collectCompareEntity(smartMaster);
        log.debug("smart proposal cost {} ms", System.currentTimeMillis() - startTime);

        buildAndCompare(compareMap, testScenarios);

        startTime = System.currentTimeMillis();
        // 4. compare layout propose result and query cube result
        RecAndQueryCompareUtil.computeCompareRank(kylinConfig, getProject(), compareMap);
        // 5. check layout
        assertOrPrintCmpResult(compareMap);
        log.debug("compare realization cost {} s", System.currentTimeMillis() - startTime);

        // 6. summary info
        val rankInfoMap = RecAndQueryCompareUtil.summarizeRankInfo(compareMap);
        StringBuilder sb = new StringBuilder();
        sb.append("All used queries: ").append(compareMap.size()).append('\n');
        rankInfoMap.forEach((key, value) -> sb.append(key).append(": ").append(value).append("\n"));
        log.debug(sb.toString());
        return compareMap;
    }

}
