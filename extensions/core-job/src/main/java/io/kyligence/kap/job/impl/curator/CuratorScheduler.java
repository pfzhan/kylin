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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionState;
import io.kyligence.kap.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceCache;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceDiscovery;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.ServiceInstance;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.details.InstanceSerializer;
import io.kyligence.kap.shaded.curator.org.apache.curator.x.discovery.details.ServiceCacheListener;

public class CuratorScheduler implements Scheduler<AbstractExecutable> {

    private static final Logger logger = LoggerFactory.getLogger(CuratorScheduler.class);
    private boolean started = false;
    private CuratorFramework curatorClient = null;
    private static CuratorLeaderSelector jobClient = null;
    private ServiceDiscovery<LinkedHashMap> serviceDiscovery = null;
    private ServiceCache<LinkedHashMap> serviceCache = null;
    private KylinConfig kylinConfig;
    private AtomicInteger count = new AtomicInteger();

    private static final String JOB_ENGINE_LEADER_PATH = "/kylin/%s/job_engine/leader";
    static final String KYLIN_SERVICE_PATH = "/kylin/%s/service";
    static final String SERVICE_NAME = "kylin";

    static final String SERVICE_PAYLOAD_DESCRIPTION = "description";

    public CuratorScheduler() {

    }

    @Override
    public void init(JobEngineConfig jobEngineConfig, JobLock jobLock) throws SchedulerException {
        kylinConfig = jobEngineConfig.getConfig();

        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        String zkAddress = kapConfig.getZookeeperConnectString();

        synchronized (this) {
            if (started) {
                logger.info("CuratorScheduler already started, skipped.");
                return;
            }

            int baseSleepTimeMs = kapConfig.getZKBaseSleepTimeMs();
            int maxRetries = kapConfig.getZKMaxRetries();
            curatorClient = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));
            logger.info("New ZK Client start: ", zkAddress);
            curatorClient.start();

            final String restAddress = KapConfig.wrap(kylinConfig).getServerRestAddress();
            try {
                registerInstance(restAddress);
            } catch (Exception e) {
                throw new SchedulerException(e);
            }

            String serverMode = jobEngineConfig.getConfig().getServerMode();
            String jobEnginePath = getJobEnginePath(slickMetadataPrefix(kylinConfig.getMetadataUrlPrefix()));
            jobClient = new CuratorLeaderSelector(curatorClient, jobEnginePath, restAddress, jobEngineConfig);
            if ("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase())) {
                try {
                    logger.info("start Job Engine, lock path is: " + jobEnginePath);
                    jobClient.start();
                    monitorJobEngine();
                } catch (IOException e) {
                    throw new SchedulerException(e);
                }
            } else {
                logger.info("server mode: " + serverMode + ", no need to run job scheduler");
            }
            started = true;
        }
    }

    private void registerInstance(String restAddress) throws Exception {
        final String host = restAddress.substring(0, restAddress.indexOf(":"));
        final String port = restAddress.substring(restAddress.indexOf(":") + 1);

        final JsonInstanceSerializer<LinkedHashMap> serializer = new JsonInstanceSerializer<>(LinkedHashMap.class);
        final String servicePath = String.format(KYLIN_SERVICE_PATH, slickMetadataPrefix(kylinConfig.getMetadataUrlPrefix()));
        serviceDiscovery = ServiceDiscoveryBuilder.builder(LinkedHashMap.class).client(curatorClient).basePath(servicePath).serializer(serializer).build();
        serviceDiscovery.start();

        serviceCache = serviceDiscovery.serviceCacheBuilder().name(SERVICE_NAME).threadFactory(Executors.defaultThreadFactory()).build();

        serviceCache.addListener(new ServiceCacheListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            }

            @Override
            public void cacheChanged() {
                logger.info("Service discovery get cacheChanged notification");
                final List<ServiceInstance<LinkedHashMap>> instances = serviceCache.getInstances();
                final List<String> instanceNodes = Lists.transform(instances, new Function<ServiceInstance<LinkedHashMap>, String>() {

                    @Nullable
                    @Override
                    public String apply(@Nullable ServiceInstance<LinkedHashMap> stringServiceInstance) {
                        return (String) stringServiceInstance.getPayload().get(SERVICE_PAYLOAD_DESCRIPTION);
                    }
                });
                final String restServersInCluster = StringUtil.join(instanceNodes, ",");
                logger.info("kylin.server.cluster-servers update to " + restServersInCluster);
                System.setProperty("kylin.server.cluster-servers", restServersInCluster);

            }
        });
        serviceCache.start();

        final LinkedHashMap<String, String> instanceDetail = new LinkedHashMap<>();
        instanceDetail.put(SERVICE_PAYLOAD_DESCRIPTION, restAddress);
        ServiceInstance<LinkedHashMap> thisInstance = ServiceInstance.<LinkedHashMap> builder().name(SERVICE_NAME).payload(instanceDetail).port(Integer.valueOf(port)).address(host).build();

        serviceDiscovery.registerService(thisInstance);
    }

    static String getJobEnginePath(String metadataUrlPrefix) {
        return String.format(JOB_ENGINE_LEADER_PATH, metadataUrlPrefix);
    }

    private void monitorJobEngine() {
        logger.info("Start collect monitor ZK Participants");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean hasLeadership = jobClient.hasLeadership();
                    boolean hasDefaultSchedulerStarted = jobClient.hasDefaultSchedulerStarted();
                    if (!(hasLeadership == hasDefaultSchedulerStarted)) {
                        logger.error("Node(" + InetAddress.getLocalHost().getHostAddress()
                                + ") job server state conflict. Is ZK leader: " + hasLeadership
                                + "; Is active job server: " + hasDefaultSchedulerStarted);
                    }

                    if (count.incrementAndGet() == 10) {
                        logger.info("Current Participants: " + jobClient.getParticipants());
                        count.set(0);
                    }
                } catch (Throwable th) {
                    logger.error("Error when getting JVM info.", th);
                }
            }
        }, 3, KapConfig.getInstanceFromEnv().getZKMonitorInterval(), TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() throws SchedulerException {
        IOUtils.closeQuietly(serviceCache);
        IOUtils.closeQuietly(serviceDiscovery);
        IOUtils.closeQuietly(curatorClient);
        IOUtils.closeQuietly(jobClient);
        started = false;
    }

    @Override
    public boolean stop(AbstractExecutable executable) throws SchedulerException {
        shutdown();
        return true;
    }

    static String slickMetadataPrefix(String metadataPrefix) {
        if (metadataPrefix.contains("/")) {
            // for local test
            return metadataPrefix.substring(metadataPrefix.lastIndexOf("/") + 1);
        }

        return metadataPrefix;
    }

    @Override
    public boolean hasStarted() {
        return started;
    }

    public static CuratorLeaderSelector getLeaderSelector() {
        return jobClient;
    }

    static class JsonInstanceSerializer<T> implements InstanceSerializer<T> {
        private final ObjectMapper mapper;
        private final Class<T> payloadClass;
        private final JavaType type;

        JsonInstanceSerializer(Class<T> payloadClass) {
            this.payloadClass = payloadClass;
            this.mapper = new ObjectMapper();

            // to bypass https://issues.apache.org/jira/browse/CURATOR-394
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            this.type = this.mapper.getTypeFactory().constructType(ServiceInstance.class);
        }

        public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
            ServiceInstance rawServiceInstance = this.mapper.readValue(bytes, this.type);
            this.payloadClass.cast(rawServiceInstance.getPayload());
            return rawServiceInstance;
        }

        public byte[] serialize(ServiceInstance<T> instance) throws Exception {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            mapper.convertValue(instance.getPayload(), payloadClass);
            this.mapper.writeValue(out, instance);
            return out.toByteArray();
        }
    }

}
