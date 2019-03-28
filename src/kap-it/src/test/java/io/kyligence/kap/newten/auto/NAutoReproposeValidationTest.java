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

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import lombok.val;

public class NAutoReproposeValidationTest extends NAutoTestBase {
    @Test
    public void test() throws Exception {
        val firstRoundProposed = new TestScenario(CompareLevel.SAME, "sql", 35, 36).execute(true);

        // set 'kap.smart.conf.rowkey.uhc.min-cardinality' = 50 to test
        getTestConfig().setProperty("kap.smart.conf.rowkey.uhc.min-cardinality", "50");
        val secondRoundProposed = new TestScenario(CompareLevel.SAME, "sql", 35, 36).execute(true);

        firstRoundProposed.forEach((sql, entity) -> {
            Assert.assertFalse(entity.getAccelerateLayouts()
                    .equalsIgnoreCase(secondRoundProposed.get(sql).getAccelerateLayouts()));
        });
    }
}
