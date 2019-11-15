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
package io.kyligence.kap.rest.config;

import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_UUID_TAG;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.UUID;

import io.kyligence.kap.rest.source.NHiveTableName;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;

import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.cluster.NodeCandidate;
import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.metric.InfluxDBWriter;
import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsController;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.initialize.AclTCRListener;
import io.kyligence.kap.rest.config.initialize.AppInitializedEvent;
import io.kyligence.kap.rest.config.initialize.BootstrapCommand;
import io.kyligence.kap.rest.config.initialize.ClusterInfoRunner;
import io.kyligence.kap.rest.config.initialize.FavoriteQueryUpdateListener;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.scheduler.EventSchedulerListener;
import io.kyligence.kap.rest.scheduler.FavoriteSchedulerListener;
import io.kyligence.kap.rest.scheduler.JobSchedulerListener;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.CacheManager;

@Slf4j
@Configuration
@Order(1)
public class AppInitializer {

    private static final String GLOBAL = "global";

    @Autowired
    TaskScheduler taskScheduler;

    @Autowired
    BootstrapCommand bootstrapCommand;

    @Autowired
    ClusterManager clusterManager;

    @Autowired
    LicenseInfoService licenseInfoService;

    @Autowired
    CacheManager cacheManager;

    @Autowired
    ClusterInfoRunner clusterInfoRunner;

    @EventListener(ApplicationPreparedEvent.class)
    public void init(ApplicationPreparedEvent event) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        NCircuitBreaker.start(KapConfig.wrap(kylinConfig));

        boolean isLeader = false;
        if (!kylinConfig.getServerMode().equals("query")) {
            val candidate = new NodeCandidate(kylinConfig.getNodeId());
            val leaderInitiator = LeaderInitiator.getInstance(kylinConfig);
            leaderInitiator.start(candidate);
            isLeader = leaderInitiator.isLeader();
        }

        if (isLeader) {
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            if (!resourceStore.exists(METASTORE_UUID_TAG)) {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    store.checkAndPutResource(METASTORE_UUID_TAG, new StringEntity(UUID.randomUUID().toString()),
                            StringEntity.serializer);
                    return null;
                }, "");
            }

            //start the embedded metrics reporters
            NMetricsController.startReporters(KapConfig.wrap(kylinConfig));

            EventListenerRegistry.getInstance(kylinConfig).register(new FavoriteQueryUpdateListener(), "fq");
            event.getApplicationContext().publishEvent(new AppInitializedEvent(event.getApplicationContext()));

            // register scheduler listener
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new EventSchedulerListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new FavoriteSchedulerListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new JobSchedulerListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new ModelBrokenListener());

            //register all global metrics
            registerGlobalMetrics(kylinConfig);
        } else {
            val auditLogStore = new JdbcAuditLogStore(kylinConfig);
            kylinConfig.setProperty("kylin.metadata.url", kylinConfig.getMetadataUrlPrefix() + "@hdfs");
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
            resourceStore.catchup();
        }

        // register acl update listener
        EventListenerRegistry.getInstance(kylinConfig).register(new AclTCRListener(cacheManager), "acl");

        // init influxDB writer and create DB
        try {
            InfluxDBWriter.getInstance();
        } catch (Exception ex) {
            log.error("InfluxDB writer has not initialized");
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void afterReady(ApplicationReadyEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.getServerMode().equals(Constant.SERVER_MODE_ALL)) {
            taskScheduler.scheduleWithFixedDelay(bootstrapCommand, 10000);
            if (kylinConfig.getLoadHiveTablenameEnabled()) {
                taskScheduler.scheduleWithFixedDelay(NHiveTableName.getInstance(),
                        kylinConfig.getLoadHiveTablenameIntervals() * 1000);
            }
        }

        String host = clusterManager.getLocalServer();
        // register host metrics
        registerHostMetrics(host);
    }

    void registerGlobalMetrics(KylinConfig config) {

        final NProjectManager projectManager = NProjectManager.getInstance(config);
        NMetricsGroup.newGauge(NMetricsName.PROJECT_GAUGE, NMetricsCategory.GLOBAL, GLOBAL, () -> {
            List<ProjectInstance> list = projectManager.listAllProjects();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        final NKylinUserManager userManager = NKylinUserManager.getInstance(config);
        NMetricsGroup.newGauge(NMetricsName.USER_GAUGE, NMetricsCategory.GLOBAL, GLOBAL, () -> {
            List<ManagedUser> list = userManager.list();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        NMetricsGroup.newCounter(NMetricsName.STORAGE_CLEAN, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.STORAGE_CLEAN_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.STORAGE_CLEAN_FAILED, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.METADATA_BACKUP, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_BACKUP_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_OPS_CRON, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_OPS_CRON_SUCCESS, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.SPARDER_RESTART, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.TRANSACTION_RETRY_COUNTER, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newHistogram(NMetricsName.TRANSACTION_LATENCY, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.BUILD_UNAVAILABLE_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.QUERY_UNAVAILABLE_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
    }

    void registerHostMetrics(String host) {
        NMetricsGroup.newCounter(NMetricsName.QUERY_HOST, NMetricsCategory.HOST, host);
        NMetricsGroup.newCounter(NMetricsName.QUERY_SCAN_BYTES_HOST, NMetricsCategory.HOST, host);
        NMetricsGroup.newHistogram(NMetricsName.QUERY_TIME_HOST, NMetricsCategory.HOST, host);

        MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
        NMetricsGroup.newGauge(NMetricsName.HEAP_MAX, NMetricsCategory.HOST, host,
                () -> mxBean.getHeapMemoryUsage().getMax());
        NMetricsGroup.newGauge(NMetricsName.HEAP_USED, NMetricsCategory.HOST, host,
                () -> mxBean.getHeapMemoryUsage().getUsed());
        NMetricsGroup.newGauge(NMetricsName.HEAP_USAGE, NMetricsCategory.HOST, host, () -> {
            final MemoryUsage usage = mxBean.getHeapMemoryUsage();
            return RatioGauge.Ratio.of(usage.getUsed(), usage.getMax()).getValue();
        });

        NMetricsGroup.newMetricSet(NMetricsName.JVM_GC, NMetricsCategory.HOST, host, new GarbageCollectorMetricSet());
        NMetricsGroup.newGauge(NMetricsName.JVM_AVAILABLE_CPU, NMetricsCategory.HOST, host,
                () -> Runtime.getRuntime().availableProcessors());

    }
}
