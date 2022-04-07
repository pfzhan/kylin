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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.rest.service;

import static io.kyligence.kap.guava20.shaded.common.net.HttpHeaders.ACCEPT_ENCODING;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.client.RestTemplate;

import com.google.common.base.CaseFormat;

import io.kyligence.kap.common.metrics.service.MonitorDao;
import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;
import io.kyligence.kap.metadata.streaming.StreamingJobStatsManager;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.tool.restclient.RestClient;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BasicService {

    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    @Autowired
    @Qualifier("userGroupService")
    protected IUserGroupService userGroupService;

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public NTableMetadataManager getTableManager(String project) {
        return NTableMetadataManager.getInstance(getConfig(), project);
    }

    public NDataModelManager getDataModelManager(String project) {
        return NDataModelManager.getInstance(getConfig(), project);
    }

    public OptRecManagerV2 getOptRecManagerV2(String project) {
        return OptRecManagerV2.getInstance(project);
    }

    public NDataflowManager getDataflowManager(String project) {
        return NDataflowManager.getInstance(getConfig(), project);
    }

    public FusionModelManager getFusionModelManager(String project) {
        return FusionModelManager.getInstance(getConfig(), project);
    }

    public NIndexPlanManager getIndexPlanManager(String project) {
        return NIndexPlanManager.getInstance(getConfig(), project);
    }

    public NDataLoadingRangeManager getDataLoadingRangeManager(String project) {
        return NDataLoadingRangeManager.getInstance(getConfig(), project);
    }

    public JobManager getJobManager(String project) {
        return JobManager.getInstance(getConfig(), project);
    }

    public NProjectManager getProjectManager() {
        return NProjectManager.getInstance(getConfig());
    }

    public NExecutableManager getExecutableManager(String project) {
        return NExecutableManager.getInstance(getConfig(), project);
    }

    public FavoriteRuleManager getFavoriteRuleManager(String project) {
        return FavoriteRuleManager.getInstance(getConfig(), project);
    }

    public SourceUsageManager getSourceUsageManager() {
        return SourceUsageManager.getInstance(getConfig());
    }

    public KafkaConfigManager getKafkaConfigManager(String project) {
        return KafkaConfigManager.getInstance(getConfig(), project);
    }

    protected static String getUsername() {
        String username = SecurityContextHolder.getContext().getAuthentication().getName();
        if (StringUtils.isEmpty(username)) {
            username = "";
        }
        return username;
    }

    public QueryHistoryDAO getQueryHistoryDao() {
        return RDBMSQueryHistoryDAO.getInstance();
    }

    public StreamingJobManager getStreamingJobManager(String project) {
        return StreamingJobManager.getInstance(getConfig(), project);
    }

    public StreamingJobStatsManager getStreamingJobStatsManager() {
        return StreamingJobStatsManager.getInstance();
    }

    public MonitorDao getMonitorDao() {
        return MonitorDao.getInstance();
    }

    public JobStatisticsManager getJobStatisticsManager(String project) {
        return JobStatisticsManager.getInstance(getConfig(), project);
    }

    public AclTCRManager getAclTCRManager(String project) {
        return AclTCRManager.getInstance(getConfig(), project);
    }

    protected static <T> Comparator<T> propertyComparator(String property, boolean ascending) {
        return new PropertyComparator<T>(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, property), false,
                ascending);

    }

    public <T> EnvelopeResponse<T> generateTaskForRemoteHost(final HttpServletRequest request, String url)
            throws Exception {
        val response = getHttpResponse(request, url);
        return JsonUtil.readValue(response.getBody(), EnvelopeResponse.class);
    }

    private ResponseEntity<byte[]> getHttpResponse(final HttpServletRequest request, String url) throws Exception {
        val body = IOUtils.toByteArray(request.getInputStream());
        HttpHeaders headers = new HttpHeaders();
        Collections.list(request.getHeaderNames())
                .forEach(k -> headers.put(k, Collections.list(request.getHeaders(k))));
        //remove gzip
        headers.remove(ACCEPT_ENCODING);
        return restTemplate.exchange(url, HttpMethod.valueOf(request.getMethod()), new HttpEntity<>(body, headers),
                byte[].class);
    }

    public Set<String> getCurrentUserGroups() {
        return userGroupService.listUserGroups(AclPermissionUtil.getCurrentUsername());
    }

    public ResourceGroupManager getResourceGroupManager() {
        return ResourceGroupManager.getInstance(getConfig());
    }

    public boolean remoteRequest(BroadcastEventReadyNotifier notifier, String projectId) {
        try {
            String projectName = notifier.getProject();
            EpochManager epochManager = EpochManager.getInstance();
            if (StringUtils.isNotBlank(projectId)) {
                projectName = getProjectManager().getProjectById(projectId).getName();
            }
            String owner = epochManager.getEpochOwner(projectName).split("\\|")[0];
            new RestClient(owner).notify(notifier);
        } catch (Exception e) {
            log.error("Failed to using rest client request.", e);
            return false;
        }
        return true;
    }
}
