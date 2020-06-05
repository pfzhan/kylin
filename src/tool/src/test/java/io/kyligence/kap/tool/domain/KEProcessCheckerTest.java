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
import io.kyligence.kap.tool.daemon.checker.KEProcessChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KEProcessCheckerTest extends NLocalFileMetadataTestCase {

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
        KEProcessChecker checker = Mockito.spy(new KEProcessChecker());

        CheckResult result = checker.check();
        Assert.assertEquals(CheckStateEnum.WARN, result.getCheckState());

        Mockito.doReturn("echo 0").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, result.getCheckState());

        Mockito.doReturn("echo 1").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.SUICIDE, result.getCheckState());

        Mockito.doReturn("echo -1").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, result.getCheckState());

        Mockito.doReturn("echo 2").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.WARN, result.getCheckState());

        Mockito.doReturn("echo -2").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.WARN, result.getCheckState());
    }

}
