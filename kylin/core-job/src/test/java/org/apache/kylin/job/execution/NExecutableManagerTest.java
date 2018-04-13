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

package org.apache.kylin.job.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

/**
 */
public class NExecutableManagerTest extends NLocalFileMetadataTestCase {

    private NExecutableManager manager;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        for (String jobPath : manager.getAllJobPathes()) {
            System.out.println("deleting " + jobPath);
            manager.deleteJob(jobPath);
        }

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        assertNotNull(manager);
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        manager.addJob(executable);
        List<AbstractExecutable> result = manager.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = manager.getJob(executable.getId());
        assertJobEqual(executable, another);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, "test output");
        assertJobEqual(executable, manager.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable1 = new SucceedTestExecutable();
        job.addTask(executable1);

        manager.addJob(job);
        assertEquals(2, job.getTasks().size());
        AbstractExecutable anotherJob = manager.getJob(job.getId());
        assertEquals(DefaultChainedExecutable.class, anotherJob.getClass());
        assertEquals(2, ((DefaultChainedExecutable) anotherJob).getTasks().size());

        job.setProject("default");
        executable.setProject("default");
        executable1.setProject("default");

        assertJobEqual(job, anotherJob);
    }

    @Test
    public void testValidStateTransfer() throws Exception {
        SucceedTestExecutable job = new SucceedTestExecutable();
        String id = job.getId();
        manager.addJob(job);
        manager.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(id, ExecutableState.ERROR, null, null);
        manager.updateJobOutput(id, ExecutableState.READY, null, null);
        manager.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(id, ExecutableState.READY, null, null);
        manager.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(id, ExecutableState.SUCCEED, null, null);
    }

    @Test(expected = IllegalStateTranferException.class)
    public void testInvalidStateTransfer() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        manager.addJob(job);
        manager.updateJobOutput(job.getId(), ExecutableState.ERROR, null, null);
        manager.updateJobOutput(job.getId(), ExecutableState.STOPPED, null, null);
    }

    private static void assertJobEqual(Executable one, Executable another) {
        assertEquals(one.getClass(), another.getClass());
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());

        assertTrue((one.getParams() == null && another.getParams() == null)
                || (one.getParams() != null && another.getParams() != null));

        if (one.getParams() != null) {
            assertEquals(one.getParams().size(), another.getParams().size());
            for (String key : one.getParams().keySet()) {
                assertEquals(one.getParams().get(key), another.getParams().get(key));
            }
        }
        if (one instanceof ChainedExecutable) {
            assertTrue(another instanceof ChainedExecutable);
            List<? extends Executable> onesSubs = ((ChainedExecutable) one).getTasks();
            List<? extends Executable> anotherSubs = ((ChainedExecutable) another).getTasks();
            assertTrue((onesSubs == null && anotherSubs == null) || (onesSubs != null && anotherSubs != null));
            if (onesSubs != null) {
                assertEquals(onesSubs.size(), anotherSubs.size());
                for (int i = 0; i < onesSubs.size(); ++i) {
                    assertJobEqual(onesSubs.get(i), anotherSubs.get(i));
                }
            }
        }
    }
}
