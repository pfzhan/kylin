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

import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.metric.InfluxDBWriter;
import io.kyligence.kap.common.metrics.NMetricsController;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.metadata.epoch.EpochOrchestrator;
import io.kyligence.kap.rest.cache.QueryCacheManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.initialize.AclTCRListener;
import io.kyligence.kap.rest.config.initialize.AfterMetadataReadyEvent;
import io.kyligence.kap.rest.config.initialize.EpochChangedListener;
import io.kyligence.kap.rest.config.initialize.FavoriteQueryUpdateListener;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.config.initialize.NMetricsRegistry;
import io.kyligence.kap.rest.scheduler.EventSchedulerListener;
import io.kyligence.kap.rest.scheduler.FavoriteSchedulerListener;
import io.kyligence.kap.rest.scheduler.JobSchedulerListener;
import io.kyligence.kap.rest.source.NHiveTableName;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;

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

    @EventListener(ApplicationPreparedEvent.class)
    public void init(ApplicationPreparedEvent event) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        NCircuitBreaker.start(KapConfig.wrap(kylinConfig));

        boolean isLeader = kylinConfig.isLeaderNode();

        if (isLeader) {
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.createMetaStoreUuidIfNotExist();
            new EpochOrchestrator(kylinConfig);

            //start the embedded metrics reporters
            NMetricsController.startReporters(KapConfig.wrap(kylinConfig));

            EventListenerRegistry.getInstance(kylinConfig).register(new FavoriteQueryUpdateListener(), "fq");

            // register scheduler listener
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new EventSchedulerListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new FavoriteSchedulerListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new JobSchedulerListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new ModelBrokenListener());
            SchedulerEventBusFactory.getInstance(kylinConfig).register(new EpochChangedListener());

            /*if (kylinConfig.getStreamingChangeMeta()) {
                // streaming change meta, skip check, just a workaround way
                resourceStore.catchup();
            }*/

        } else {
            val auditLogStore = new JdbcAuditLogStore(kylinConfig);
            kylinConfig.setProperty("kylin.metadata.url", kylinConfig.getMetadataUrlPrefix() + "@hdfs");
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
            resourceStore.catchup();
        }
        event.getApplicationContext().publishEvent(new AfterMetadataReadyEvent(event.getApplicationContext()));

        // register acl update listener
        EventListenerRegistry.getInstance(kylinConfig).register(new AclTCRListener(queryCacheManager), "acl");

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
        if (kylinConfig.isLeaderNode()) {
            if (kylinConfig.getLoadHiveTablenameEnabled()) {
                taskScheduler.scheduleWithFixedDelay(NHiveTableName.getInstance(),
                        kylinConfig.getLoadHiveTablenameIntervals() * 1000);
            }
        }

        String host = clusterManager.getLocalServer();
        // register host metrics
        NMetricsRegistry.registerHostMetrics(host);
    }

}
