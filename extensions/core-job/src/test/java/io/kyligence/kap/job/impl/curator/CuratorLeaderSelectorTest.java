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

package io.kyligence.kap.job.impl.curator;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.Participant;
import io.kyligence.kap.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import io.kyligence.kap.shaded.curator.org.apache.curator.test.TestingServer;

public class CuratorLeaderSelectorTest extends LocalFileMetadataTestCase {
    private TestingServer zkTestServer;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer();
        zkTestServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        System.setProperty("kylin.server.mode", "all");
        createTestMetadata();
    }

    @Test
    public void testGetBasic() throws SchedulerException, IOException, InterruptedException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final String zkString = zkTestServer.getConnectString();
        final String server1 = "server1:1111";
        final String server2 = "server2:2222";
        String jobEnginePath = CuratorScheduler
                .getJobEnginePath(CuratorScheduler.slickMetadataPrefix(kylinConfig.getMetadataUrlPrefix()));
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkString, new ExponentialBackoffRetry(3000, 3));
        client.start();
        CuratorLeaderSelector s1 = new CuratorLeaderSelector(client //
                , jobEnginePath //
                , server1 //
                , new JobEngineConfig(kylinConfig)); //
        Assert.assertFalse(s1.hasDefaultSchedulerStarted());
        CuratorLeaderSelector s2 = new CuratorLeaderSelector(client //
                , jobEnginePath //
                , server2 //
                , new JobEngineConfig(kylinConfig)); //
        s1.start();
        //wait for Selector starting
        Thread.sleep(1000);
        Assert.assertEquals(1, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());
        s2.start();
        Thread.sleep(1000);
        Assert.assertEquals(2, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());

        Assert.assertEquals(new Participant(server1, true), s1.getLeader());
        Assert.assertEquals(s1.getLeader(), s2.getLeader());
        Assert.assertTrue(s1.hasDefaultSchedulerStarted());
        s1.close();
        Thread.sleep(1000);
        Assert.assertEquals(1, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());
        Assert.assertEquals(new Participant(server2, true), s1.getLeader());
        Assert.assertTrue(s2.hasDefaultSchedulerStarted());
        s2.close();
        Thread.sleep(1000);
        Assert.assertEquals(0, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());
    }

    @After
    public void after() throws Exception {
        zkTestServer.close();
        cleanupTestMetadata();
        System.clearProperty("kylin.env.zookeeper-connect-string");
        System.clearProperty("kap.server.host-address");
        System.clearProperty("kylin.server.mode");
    }
}
