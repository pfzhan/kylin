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
package io.kyligence.kap.tool.domain;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateEnum;
import io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static io.kyligence.kap.tool.daemon.CheckStateEnum.NORMAL;
import static io.kyligence.kap.tool.daemon.CheckStateEnum.RESTART;
import static io.kyligence.kap.tool.daemon.CheckStateEnum.WARN;
import static io.kyligence.kap.tool.daemon.CheckStateEnum.QUERY_UPGRADE;
import static io.kyligence.kap.tool.daemon.CheckStateEnum.QUERY_DOWNGRADE;

public class FullGCDurationCheckerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        FullGCDurationChecker checker = Mockito.spy(new FullGCDurationChecker());

        CheckResult checkResult = checker.check();
        Assert.assertEquals(WARN, checkResult.getCheckState());

        double[] gcTimes = new double[] { 1.0, 1.0, 1.0, 1.0, 1.0, 20.0, 21.0, 40.0, 41.0, 60.0, 95.0, 80.0, 80.0, 80.0,
                80.0, 99.0 };
        CheckStateEnum[] states = new CheckStateEnum[] { NORMAL, NORMAL, NORMAL, NORMAL, NORMAL, QUERY_UPGRADE, NORMAL,
                NORMAL, QUERY_DOWNGRADE, QUERY_DOWNGRADE, RESTART, QUERY_DOWNGRADE, QUERY_DOWNGRADE, NORMAL, NORMAL,
                QUERY_UPGRADE };

        long now = System.currentTimeMillis();
        for (int i = 0; i < gcTimes.length; i++) {
            Mockito.doReturn(now).when(checker).getNowTime();
            Mockito.doReturn(gcTimes[i]).when(checker).getGCTime();
            checkResult = checker.check();
            Assert.assertEquals(states[i], checkResult.getCheckState());
            now += 20 * 1000;
        }
    }

}
