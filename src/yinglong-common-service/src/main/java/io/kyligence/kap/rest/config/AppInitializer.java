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

import java.util.Date;
import java.util.List;

import org.apache.commons.jnet.Installer;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;

import io.kyligence.kap.common.constant.Constant;
import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.common.util.HostInfoFetcher;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.metadata.epoch.EpochOrchestrator;
import io.kyligence.kap.metadata.project.NProjectLoader;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.streaming.JdbcStreamingJobStatsStore;
import io.kyligence.kap.rest.config.initialize.AclTCRListener;
import io.kyligence.kap.rest.config.initialize.AfterMetadataReadyEvent;
import io.kyligence.kap.rest.config.initialize.CacheCleanListener;
import io.kyligence.kap.rest.config.initialize.EpochChangedListener;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.config.initialize.ProcessStatusListener;
import io.kyligence.kap.rest.config.initialize.QueryMetricsListener;
import io.kyligence.kap.rest.config.initialize.SparderStartEvent;
import io.kyligence.kap.rest.config.initialize.TableSchemaChangeListener;
import io.kyligence.kap.rest.service.CommonQueryCacheSupporter;
import io.kyligence.kap.rest.util.JStackDumpTask;
import io.kyligence.kap.streaming.jobs.StreamingJobListener;
import io.kyligence.kap.tool.daemon.KapGuardianHATask;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Order(1)
public class AppInitializer {

    @Autowired
    TaskScheduler taskScheduler;

    @Autowired(required = false)
    CommonQueryCacheSupporter queryCacheManager;

    @Autowired(required = false)
    EpochChangedListener epochChangedListener;

    @Autowired(required = false)
    HostInfoFetcher hostInfoFetcher;

    JdbcStreamingJobStatsStore streamingJobStatsStore;

    @EventListener(ApplicationPreparedEvent.class)
    public void init(ApplicationPreparedEvent event) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        NCircuitBreaker.start(KapConfig.wrap(kylinConfig));

        boolean isJob = kylinConfig.isJobNode();
        boolean isDataLoading = kylinConfig.isDataLoadingNode();
        boolean isMetadata = kylinConfig.isMetadataNode();

        // set kylin.metadata.distributed-lock.jdbc.url
        // before kylin.metadata.url is changed
        kylinConfig.setJDBCDistributedLockURL(kylinConfig.getJDBCDistributedLockURL().toString());

        if (isJob || isDataLoading || isMetadata) {
            // restore from metadata, should not delete
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.setChecker((e) -> {
                String instance = e.getInstance();
                String localIdentify = EpochOrchestrator.getOwnerIdentity().split("\\|")[0];
                return localIdentify.equalsIgnoreCase(instance);
            });
            if (isJob || isDataLoading) {
                streamingJobStatsStore = new JdbcStreamingJobStatsStore(kylinConfig);

                // register scheduler listener
                EventBusFactory.getInstance().register(new StreamingJobListener(), true);
            }
            if (isJob || isMetadata) {
                EventBusFactory.getInstance().register(new ModelBrokenListener(), false);
                EventBusFactory.getInstance().register(epochChangedListener, false);
            }
            EventBusFactory.getInstance().register(new ProcessStatusListener(), true);

            ExecutableUtils.initJobFactory();
        } else {
            val auditLogStore = new JdbcAuditLogStore(kylinConfig);
            val epochStore = EpochStore.getEpochStore(kylinConfig);
            kylinConfig.setQueryHistoryUrl(kylinConfig.getQueryHistoryUrl().toString());
            kylinConfig.setStreamingStatsUrl(kylinConfig.getStreamingStatsUrl().toString());
            if (kylinConfig.getMetadataStoreType().equals("hdfs")) {
                kylinConfig.setProperty("kylin.metadata.url", kylinConfig.getMetadataUrlPrefix() + "@hdfs");
            }
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
            resourceStore.catchup();
            resourceStore.getMetadataStore().setEpochStore(epochStore);
        }

        kylinConfig.getDistributedLockFactory().initialize();
        warmUpSystemCache();
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
        // register for clean cache when delete
        EventListenerRegistry.getInstance(kylinConfig).register(new CacheCleanListener(), "cacheInManager");

        EventBusFactory.getInstance().register(new QueryMetricsListener(), false);

        postInit();
    }

    private void warmUpSystemCache() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> prjInstances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        prjInstances.forEach(prjInstance -> {
            NProjectLoader.updateCache(prjInstance.getName());
            NProjectLoader.removeCache();
        });
    }

    @EventListener(ApplicationReadyEvent.class)
    public void afterReady(ApplicationReadyEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        setFsUrlStreamHandlerFactory();
        if (kylinConfig.isJobNode()|| kylinConfig.isMetadataNode()) {
            new EpochOrchestrator(kylinConfig);
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
    }

    private void postInit() {
        AddressUtil.setHostInfoFetcher(hostInfoFetcher);
    }

    private static void setFsUrlStreamHandlerFactory() {
        try {
            Installer.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Exception e) {
            log.warn("set Fs URL stream handler factory failed", e);
        }
    }
}
