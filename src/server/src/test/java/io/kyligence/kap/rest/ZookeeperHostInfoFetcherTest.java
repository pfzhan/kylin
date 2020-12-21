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

package io.kyligence.kap.rest;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.util.AbstractTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import io.kyligence.kap.common.util.HostInfoFetcher;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { HostInfoFetcher.class, ZookeeperHostInfoFetcher.class })
@TestPropertySource(properties = "spring.cloud.zookeeper.enabled=true")
@EnableAutoConfiguration
public class ZookeeperHostInfoFetcherTest extends AbstractTestCase {

    @Autowired
    HostInfoFetcher hostInfoFetcher;

    TestingServer zkTestServer;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer(true);
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
    }

    @After
    public void teardown() throws IOException {
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    @Test
    public void getHostname() {
        Assert.assertNotNull(hostInfoFetcher);
        Assert.assertEquals(hostInfoFetcher.getClass().getName(), ZookeeperHostInfoFetcher.class.getName());
        Assert.assertNotNull(hostInfoFetcher.getHostname());
    }
}