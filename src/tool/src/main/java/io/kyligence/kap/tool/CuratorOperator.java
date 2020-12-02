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
package io.kyligence.kap.tool;

import static org.apache.kylin.common.ZookeeperConfig.geZKClientConnectionTimeoutMs;
import static org.apache.kylin.common.ZookeeperConfig.geZKClientSessionTimeoutMs;
import static org.apache.kylin.common.ZookeeperConfig.getZKConnectString;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.ZookeeperAclBuilder;
import org.apache.zookeeper.data.Stat;

import io.kyligence.kap.common.util.ClusterConstant;
import io.kyligence.kap.tool.discovery.ServiceInstanceSerializer;
import io.kyligence.kap.tool.kerberos.KerberosLoginTask;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CuratorOperator implements AutoCloseable {

    private CuratorFramework zkClient;

    public CuratorOperator() {
        KerberosLoginTask kerberosLoginTask = new KerberosLoginTask();
        kerberosLoginTask.execute();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String connectString = getZKConnectString();
        ZookeeperAclBuilder aclBuilder = new ZookeeperAclBuilder().invoke();
        zkClient = aclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder()).connectString(connectString)
                .sessionTimeoutMs(geZKClientSessionTimeoutMs()).connectionTimeoutMs(geZKClientConnectionTimeoutMs())
                .retryPolicy(retryPolicy).build();
        zkClient.start();
    }

    public boolean isJobNodeExist() throws Exception {
        return checkNodeExist(ClusterConstant.ALL) || checkNodeExist(ClusterConstant.JOB);
    }

    private boolean checkNodeExist(String serverMode) throws Exception {
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
        String nodePath = "/kylin/" + identifier + "/services/" + serverMode;
        Stat stat = zkClient.checkExists().forPath(nodePath);
        if (stat == null) {
            return false;
        }
        List<String> childNodes = zkClient.getChildren().forPath(nodePath);
        return childNodes != null && !childNodes.isEmpty();
    }

    @Override
    public void close() throws Exception {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    public static void main(String[] args) {
        int ret = 0;
        try (val curatorOperator = new CuratorOperator()) {
            if (curatorOperator.isJobNodeExist()) {
                ret = 1;
            }
        } catch (Exception e) {
            log.error("", e);
            ret = 1;
        }
        System.exit(ret);
    }

    public String getAddress() throws Exception {
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
        ServiceDiscovery serviceDiscovery = ServiceDiscoveryBuilder.builder(Object.class).client(zkClient)
                .basePath("/kylin/" + identifier + "/services")
                .serializer(new ServiceInstanceSerializer<>(Object.class)).build();
        serviceDiscovery.start();

        ServiceProvider provider = serviceDiscovery.serviceProviderBuilder().serviceName("all")
                .providerStrategy(new RandomStrategy<>()).build();
        provider.start();

        ServiceInstance serviceInstance = provider.getInstance();

        if (serviceInstance == null) {
            return null;
        }
        return serviceInstance.getAddress() + ":" + serviceInstance.getPort();
    }
}
