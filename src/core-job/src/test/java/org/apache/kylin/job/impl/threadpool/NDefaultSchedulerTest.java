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

package org.apache.kylin.job.impl.threadpool;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.job.execution.FailedTestExecutable;
import org.apache.kylin.job.execution.ErrorTestExecutable;
import org.apache.kylin.job.execution.SelfStopExecutable;
import org.apache.kylin.job.execution.NoErrorStatusExecutable;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NDefaultSchedulerTest extends BaseSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultSchedulerTest.class);

    public NDefaultSchedulerTest() {
        super("default");
    }

    @Override
    public void after() throws Exception {
        super.after();
        System.clearProperty("kylin.job.retry");
        System.clearProperty("kylin.job.retry-exception-classes");
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSingleTaskJob() throws Exception {
        logger.info("testSingleTaskJob");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        job.addTask(task1);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSucceed() throws Exception {
        logger.info("testSucceed");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndFailed() throws Exception {
        logger.info("testSucceedAndFailed");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new FailedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndError() throws Exception {
        logger.info("testSucceedAndError");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new ErrorTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testDiscard() throws Exception {
        logger.info("testDiscard");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        SelfStopExecutable task1 = new SelfStopExecutable();
        job.addTask(task1);
        executableManager.addJob(job);
        Thread.sleep(1100); // give time to launch job/task1 
        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);
        executableManager.discardJob(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(task1.getId()).getState());
        task1.waitForDoWork();
    }

    @Test
    public void testIllegalState() throws Exception {
        logger.info("testIllegalState");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        BaseTestExecutable task1 = new SucceedTestExecutable();
        BaseTestExecutable task2 = new SucceedTestExecutable();
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateJobOutput(task2.getId(),
                ExecutableState.RUNNING, null, null);
        waitForJobFinish(job.getId(), 10000);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testMetaStoreRecover() throws Exception {
        logger.info("testMetaStoreRecover");
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        ErrorTestExecutable task = new ErrorTestExecutable();
        job.addTask(task);
        executableManager.addJob(job);
        Thread.sleep(2000);
        runningJobToError(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
    }

    @Test
    public void testSchedulerStop() throws Exception {
        logger.info("testSchedulerStop");

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("too long wait time");

        DefaultChainedExecutable job = new DefaultChainedExecutable();
        FiveSecondSucceedTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        job.addTask(task1);
        executableManager.addJob(job);

        //scheduler failed due to some reason
        scheduler.shutdown();

        waitForJobFinish(job.getId(), 6000);
    }

    @Test
    public void testSchedulerRestart() throws Exception {
        logger.info("testSchedulerRestart");

        DefaultChainedExecutable job = new DefaultChainedExecutable();
        FiveSecondSucceedTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        job.addTask(task1);
        executableManager.addJob(job);

        //sleep 3s to make sure SucceedTestExecutable is running 
        Thread.sleep(3000);
        //scheduler failed due to some reason
        scheduler.shutdown();
        //restart
        startScheduler();

        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testRetryableException() throws Exception {
        System.setProperty("kylin.job.retry", "1");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        Assert.assertTrue(executable.needRetry());
    }
}
