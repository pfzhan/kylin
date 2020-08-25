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

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.date.Constant;
import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.metrics.NMetricsController;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.epoch.EpochOrchestrator;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.broadcaster.BroadcastListener;
import io.kyligence.kap.rest.cache.QueryCacheManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.initialize.AclTCRListener;
import io.kyligence.kap.rest.config.initialize.AfterMetadataReadyEvent;
import io.kyligence.kap.rest.config.initialize.EpochChangedListener;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.config.initialize.NMetricsRegistry;
import io.kyligence.kap.rest.config.initialize.SourceUsageUpdateListener;
import io.kyligence.kap.rest.config.initialize.SparderStartEvent;
import io.kyligence.kap.rest.config.initialize.TableSchemaChangeListener;
import io.kyligence.kap.rest.scheduler.JobSchedulerListener;
import io.kyligence.kap.rest.service.NQueryHistoryScheduler;
import io.kyligence.kap.rest.source.NHiveTableName;
import io.kyligence.kap.rest.util.JStackDumpTask;
import io.kyligence.kap.tool.daemon.KapGuardianHATask;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Order(1)
public class AppInitializer {

    @Autowired
    TaskScheduler taskScheduler;

    @Autowired
    ClusterManager clusterManager;

    @Autowired
    QueryCacheManager queryCacheManager;

    @Autowired
    EpochChangedListener epochChangedListener;

    @Autowired
    BroadcastListener broadcastListener;

    @Autowired
    SourceUsageUpdateListener sourceUsageUpdateListener;

    private static final ScheduledExecutorService METRICS_SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("MetricsChecker"));

    private static final Set<String> allControlledProjects = Collections.synchronizedSet(new HashSet<>());

    @EventListener(ApplicationPreparedEvent.class)
    public void init(ApplicationPreparedEvent event) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        NCircuitBreaker.start(KapConfig.wrap(kylinConfig));

        boolean isJob = kylinConfig.isJobNode();

        //start the embedded metrics reporters
        NMetricsController.startReporters(KapConfig.wrap(kylinConfig));

        if (isJob) {
            // restore from metadata, should not delete
            ResourceStore.getKylinMetaStore(kylinConfig);

            // register scheduler listener
            EventBusFactory.getInstance().register(new JobSchedulerListener());
            EventBusFactory.getInstance().register(new ModelBrokenListener());
            EventBusFactory.getInstance().register(epochChangedListener);
            EventBusFactory.getInstance().register(broadcastListener);
            EventBusFactory.getInstance().register(sourceUsageUpdateListener);

            ExecutableUtils.initJobFactory();
        } else {
            val auditLogStore = new JdbcAuditLogStore(kylinConfig);
            val epochStore = EpochStore.getEpochStore(kylinConfig);
            kylinConfig.setProperty("kylin.metadata.url", kylinConfig.getMetadataUrlPrefix() + "@hdfs");
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
            resourceStore.catchup();
            resourceStore.getMetadataStore().setEpochStore(epochStore);
        }
        event.getApplicationContext().publishEvent(new AfterMetadataReadyEvent(event.getApplicationContext()));

        if (kylinConfig.isQueryNode()) {
            if (kylinConfig.isSparderAsync()) {
                event.getApplicationContext()
                        .publishEvent(new SparderStartEvent.AsyncEvent(event.getApplicationContext()));
            } else {
                event.getApplicationContext()
                        .publishEvent(new SparderStartEvent.SyncEvent(event.getApplicationContext()));
            }
        }
        // register acl update listener
        EventListenerRegistry.getInstance(kylinConfig).register(new AclTCRListener(queryCacheManager), "acl");
        // register schema change listener
        EventListenerRegistry.getInstance(kylinConfig).register(new TableSchemaChangeListener(queryCacheManager),
                "table");
        try {
            NQueryHistoryScheduler queryHistoryScheduler = NQueryHistoryScheduler.getInstance();
            queryHistoryScheduler.init();
        } catch (Exception ex) {
            log.error("NQueryHistoryScheduler init fail");
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void afterReady(ApplicationReadyEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isJobNode()) {
            new EpochOrchestrator(kylinConfig);
            EpochManager epochManager = EpochManager.getInstance(kylinConfig);
            epochManager.updateOwnedEpoch();
            if (kylinConfig.getLoadHiveTablenameEnabled()) {
                taskScheduler.scheduleWithFixedDelay(NHiveTableName.getInstance(),
                        kylinConfig.getLoadHiveTablenameIntervals() * Constant.SECOND);
            }
        }

        if (kylinConfig.getJStackDumpTaskEnabled()) {
            taskScheduler.scheduleAtFixedRate(new JStackDumpTask(),
                    kylinConfig.getJStackDumpTaskPeriod() * Constant.MINUTE);
        }

        if (kylinConfig.isGuardianEnabled() && kylinConfig.isGuardianHAEnabled()) {
            log.info("Guardian Process ha is enabled, start check scheduler");
            taskScheduler.scheduleAtFixedRate(new KapGuardianHATask(),
                    new Date(System.currentTimeMillis() + kylinConfig.getGuardianHACheckInitDelay() * Constant.SECOND),
                    kylinConfig.getGuardianHACheckInterval() * Constant.SECOND);
        }

        registerMetrics();
    }

    /**
     * register all metrics
     */
    private void registerMetrics() {
        String host = clusterManager.getLocalServer();

        log.info("Register global metrics...");
        NMetricsRegistry.registerGlobalMetrics(KylinConfig.getInstanceFromEnv(), host);

        log.info("Register host metrics...");
        NMetricsRegistry.registerHostMetrics(host);

        METRICS_SCHEDULED_EXECUTOR.scheduleAtFixedRate(() -> {
            Set<String> allProjects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                    .stream().map(ProjectInstance::getName).collect(Collectors.toSet());

            Sets.SetView<String> newProjects = Sets.difference(allProjects, allControlledProjects);
            for (String newProject : newProjects) {
                log.info("Register project metrics for {}", newProject);
                NMetricsRegistry.registerProjectMetrics(KylinConfig.getInstanceFromEnv(), newProject, host);
            }

            Sets.SetView<String> outDatedProjects = Sets.difference(allControlledProjects, allProjects);

            for (String outDatedProject : outDatedProjects) {
                log.info("Remove project metrics for {}", outDatedProject);
                NMetricsGroup.removeProjectMetrics(outDatedProject);
            }

            allControlledProjects.clear();
            allControlledProjects.addAll(allProjects);

        }, 1, 1, TimeUnit.MINUTES);
    }
}
