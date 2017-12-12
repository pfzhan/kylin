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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import io.kyligence.kap.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import io.kyligence.kap.shaded.curator.org.apache.curator.test.TestingServer;
import io.kyligence.kap.shaded.curator.org.apache.curator.utils.CloseableUtils;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceDiscovery;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceInstance;

/**
 */
public class CuratorSchedulerTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CuratorSchedulerTest.class);

    private TestingServer zkTestServer;

    protected ExecutableManager jobService;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer();
        zkTestServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        System.setProperty("kylin.server.mode", "query");
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        zkTestServer.close();
        cleanupTestMetadata();
        System.clearProperty("kylin.env.zookeeper-connect-string");
        System.clearProperty("kap.server.host-address");
        System.clearProperty("kylin.server.cluster-servers");
        System.clearProperty("kylin.server.mode");
    }

    @Test
    public void test() throws Exception {

        final String zkString = zkTestServer.getConnectString();
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        ServiceDiscovery<LinkedHashMap> serviceDiscovery = null;
        CuratorFramework curatorClient = null;
        try {

            final CuratorScheduler.JsonInstanceSerializer<LinkedHashMap> serializer = new CuratorScheduler.JsonInstanceSerializer<>(
                    LinkedHashMap.class);
            String servicePath = String.format(CuratorScheduler.KYLIN_SERVICE_PATH,
                    CuratorScheduler.slickMetadataPrefix(kylinConfig.getMetadataUrlPrefix()));
            curatorClient = CuratorFrameworkFactory.newClient(zkString, new ExponentialBackoffRetry(3000, 3));
            curatorClient.start();
            serviceDiscovery = ServiceDiscoveryBuilder.builder(LinkedHashMap.class).client(curatorClient)
                    .basePath(servicePath).serializer(serializer).build();
            serviceDiscovery.start();

            final ExampleServer server1 = new ExampleServer("localhost:1111");
            final ExampleServer server2 = new ExampleServer("localhost:2222");

            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            Assert.assertTrue(serviceNames.size() == 1);
            Assert.assertTrue(CuratorScheduler.SERVICE_NAME.equals(serviceNames.iterator().next()));
            Collection<ServiceInstance<LinkedHashMap>> instances = serviceDiscovery
                    .queryForInstances(CuratorScheduler.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 2);
            List<ServiceInstance<LinkedHashMap>> instancesList = Lists.newArrayList(instances);

            final List<String> instanceNodes = Lists.transform(instancesList,
                    new Function<ServiceInstance<LinkedHashMap>, String>() {

                        @Nullable
                        @Override
                        public String apply(@Nullable ServiceInstance<LinkedHashMap> stringServiceInstance) {
                            return (String) stringServiceInstance.getPayload()
                                    .get(CuratorScheduler.SERVICE_PAYLOAD_DESCRIPTION);
                        }
                    });

            Assert.assertTrue(instanceNodes.contains(server1.getAddress()));
            Assert.assertTrue(instanceNodes.contains(server2.getAddress()));

            // stop one server
            server1.close();
            instances = serviceDiscovery.queryForInstances(CuratorScheduler.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 1);
            Assert.assertEquals(server2.getAddress(),
                    instances.iterator().next().getPayload().get(CuratorScheduler.SERVICE_PAYLOAD_DESCRIPTION));

            // all stop
            server2.close();
            instances = serviceDiscovery.queryForInstances(CuratorScheduler.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 0);

        } finally {
            CloseableUtils.closeQuietly(serviceDiscovery);
            CloseableUtils.closeQuietly(curatorClient);
        }

    }

}
