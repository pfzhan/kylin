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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.metrics.MetricsController;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.initialize.MetricsRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class MetricsConfig {
    @Autowired
    ClusterManager clusterManager;

    private static final ScheduledExecutorService METRICS_SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(2,
            new NamedThreadFactory("MetricsChecker"));

    private static final Set<String> allControlledProjects = Collections.synchronizedSet(new HashSet<>());

    @EventListener(ApplicationReadyEvent.class)
    public void registerMetrics() {
        String host = clusterManager.getLocalServer();

        log.info("Register global metrics...");
        MetricsRegistry.registerGlobalMetrics(KylinConfig.getInstanceFromEnv(), host);

        log.info("Register host metrics...");
        MetricsRegistry.registerHostMetrics(host);
        if (KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
            log.info("Register prometheus global metrics... ");
            MetricsRegistry.registerGlobalPrometheusMetrics();
        }

        METRICS_SCHEDULED_EXECUTOR.scheduleAtFixedRate(() -> {
            Set<String> allProjects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                    .stream().map(ProjectInstance::getName).collect(Collectors.toSet());
            
            Sets.SetView<String> newProjects = Sets.difference(allProjects, allControlledProjects);
            for (String newProject : newProjects) {
                log.info("Register project metrics for {}", newProject);
                MetricsRegistry.registerProjectMetrics(KylinConfig.getInstanceFromEnv(), newProject, host);
                MetricsRegistry.registerProjectPrometheusMetrics(KylinConfig.getInstanceFromEnv(), newProject);
            }

            Sets.SetView<String> outDatedProjects = Sets.difference(allControlledProjects, allProjects);

            for (String outDatedProject : outDatedProjects) {
                log.info("Remove project metrics for {}", outDatedProject);
                MetricsGroup.removeProjectMetrics(outDatedProject);
                MetricsRegistry.removeProjectFromStorageSizeMap(outDatedProject);
            }

            allControlledProjects.clear();
            allControlledProjects.addAll(allProjects);

        }, 0, 1, TimeUnit.MINUTES);

        METRICS_SCHEDULED_EXECUTOR.scheduleAtFixedRate(MetricsRegistry::refreshTotalStorageSize,
                0, 10, TimeUnit.MINUTES);

        METRICS_SCHEDULED_EXECUTOR.execute(() -> MetricsController.startReporters(KapConfig.wrap(KylinConfig.getInstanceFromEnv())));
    }
}
