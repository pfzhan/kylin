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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.daemon.CheckResult;
import io.kyligence.kap.tool.daemon.CheckStateEnum;
import io.kyligence.kap.tool.daemon.checker.KEStatusChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KEStatusCheckerTest extends NLocalFileMetadataTestCase {

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
        KEStatusChecker checker = Mockito.spy(new KEStatusChecker());

        CheckResult checkResult;
        for (int i = 0; i < 4; i++) {
            checkResult = checker.check();
            Assert.assertEquals(CheckStateEnum.WARN, checkResult.getCheckState());
        }

        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, checkResult.getCheckState());

        KEStatusChecker.SparkStatus sparkStatus = new KEStatusChecker.SparkStatus(0, 0);
        KEStatusChecker.CanceledSlowQueryStatus slowQueryStatus = new KEStatusChecker.CanceledSlowQueryStatus("1", 1,
                System.currentTimeMillis(), 400);
        KEStatusChecker.Status status = new KEStatusChecker.Status(sparkStatus, Lists.newArrayList(slowQueryStatus));
        KEStatusChecker.EnvelopeResponse response = new KEStatusChecker.EnvelopeResponse<>("000", status, "");

        Mockito.doReturn(response).when(checker).getHealthStatus();
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, checkResult.getCheckState());

        sparkStatus.setFailureTimes(3);
        sparkStatus.setLastFailureTime(System.currentTimeMillis());
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, checkResult.getCheckState());
        sparkStatus.setFailureTimes(0);

        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, checkResult.getCheckState());

        slowQueryStatus.setCanceledTimes(3);
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, checkResult.getCheckState());

        slowQueryStatus.setCanceledTimes(1);
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, checkResult.getCheckState());
    }

}
