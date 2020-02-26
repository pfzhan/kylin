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
package io.kyligence.kap.rest.config.initialize;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.kyligence.kap.rest.response.ServerInfoResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.metrics.context.ClusterContext;
import io.kyligence.kap.common.metrics.context.ClusterInfo;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Component
public class ClusterInfoRunner implements Runnable {

    private static final String GLOBAL = "global";
    private static final String HEALTHY = "UP";
    private static final String HEALTH_URL_TEMPLATE = "http://%s/kylin/api/health";

    @Autowired
    private ClusterManager clusterManager;
    @Autowired
    private ClusterContext clusterContext;

    @Override
    public void run() {
        updateHostUnavailableDuration(NMetricsName.QUERY_UNAVAILABLE_DURATION, clusterContext.getQueryInfo(),
                this::isQueryAvailable);
        updateHostUnavailableDuration(NMetricsName.BUILD_UNAVAILABLE_DURATION, clusterContext.getAllInfo(),
                this::isBuildAvailable);
    }

    private boolean isBuildAvailable() {
        // first check MetaStore and FileSystem and SparkSqlContext
        String server = clusterManager.getLocalServer();
        String url = String.format(HEALTH_URL_TEMPLATE, server);
        if (checkHealth(url)) {
            return false;
        }

        // then check the ratio of error jobs
        int jobCount = 0;
        int errJobCount = 0;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            List<AbstractExecutable> executables = NExecutableManager.getInstance(config, projectInstance.getName())
                    .getAllExecutables();
            jobCount += executables.size();
            errJobCount += executables.stream().filter(executable -> executable.getStatus() == ExecutableState.ERROR)
                    .count();
        }
        return errJobCount <= jobCount * 0.7d;
    }

    private boolean isQueryAvailable() {
        List<String> queryServers = clusterManager.getQueryServers().stream().map(ServerInfoResponse::getHost)
                .collect(Collectors.toList());
        for (String queryServer : queryServers) {
            String url = String.format(HEALTH_URL_TEMPLATE, queryServer);
            if (checkHealth(url)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkHealth(String url) {
        val restTemplate = new RestTemplate();
        try {
            Map ret = restTemplate.getForObject(url, Map.class);
            if (HEALTHY.equals(ret.get("status"))) {
                return true;
            }
        } catch (HttpStatusCodeException e) {
            log.error("health check failed.", e);
        }
        return false;
    }

    private void updateHostUnavailableDuration(NMetricsName name, ClusterInfo clusterInfo, Predicate predicate) {
        long current = System.currentTimeMillis();
        if (clusterInfo.isUnavailable()) {
            NMetricsGroup.counterInc(name, NMetricsCategory.GLOBAL, GLOBAL,
                    current - clusterInfo.getLastUnavailableTime());
            if (predicate.isAvailable()) {
                clusterInfo.setUnavailable(false);
            } else {
                clusterInfo.setLastUnavailableTime(current);
            }
        } else {
            if (!predicate.isAvailable()) {
                clusterInfo.setUnavailable(true);
                clusterInfo.setLastUnavailableTime(current);
            }
        }
    }

    interface Predicate {
        boolean isAvailable();
    }
}
