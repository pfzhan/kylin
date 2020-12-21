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

package io.kyligence.kap.rest.config.initialize;

import static org.awaitility.Awaitility.await;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.execution.NExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.ProcessUtils;
import lombok.val;

public class ProcessStatusListenerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    @Ignore
    public void testKillProcess() {
        EventBusFactory.getInstance().register(new ProcessStatusListener(), true);
        val executableManager = NExecutableManager.getInstance(getTestConfig(), "default");
        final String jobId = "job000000001";
        final String execCmd = "nohup sleep 30 & sleep 30";

        Thread execThread = new Thread(() -> {
            CliCommandExecutor exec = new CliCommandExecutor();
            try {
                exec.execute(execCmd, null, jobId);
            } catch (ShellException e) {
                // do nothing
                e.printStackTrace();
            }
        });
        execThread.start();

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            val jobMap = ProcessStatusListener.parseProcessFile();
            val pid = jobMap.entrySet().stream().filter(entry -> entry.getValue().equals(jobId)).map(Map.Entry::getKey)
                    .findFirst();
            Assert.assertTrue(pid.isPresent());
            Assert.assertTrue(ProcessUtils.isAlive(pid.get()));
        });

        overwriteSystemProp("KYLIN_HOME", Paths.get(System.getProperty("user.dir")).getParent().getParent() + "/build");
        executableManager.destroyProcess(jobId);

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            val jobMap = ProcessStatusListener.parseProcessFile();
            val pid = jobMap.entrySet().stream().filter(entry -> entry.getValue().equals(jobId)).map(Map.Entry::getKey)
                    .findFirst();
            Assert.assertFalse(pid.isPresent());
        });

    }
}
