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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import io.kyligence.kap.rest.response.ProjectConfigResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import lombok.var;

@Component("projectService")
public class ProjectService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private MetadataBackupService metadataBackupService;

    public ProjectInstance deserializeProjectDesc(ProjectRequest projectRequest) {
        logger.debug("Saving project " + projectRequest.getProjectDescData());
        ProjectInstance projectDesc;
        try {
            projectDesc = JsonUtil.readValue(projectRequest.getProjectDescData(), ProjectInstance.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return projectDesc;
    }

    @Transaction
    public ProjectInstance createProject(ProjectInstance newProject) {
        Message msg = MsgPicker.getMsg();
        String projectName = newProject.getName();
        String description = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();
        ProjectInstance currentProject = getProjectManager().getProject(projectName);
        if (currentProject != null) {
            throw new BadRequestException(String.format(msg.getPROJECT_ALREADY_EXIST(), projectName));
        }
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        ProjectInstance createdProject = getProjectManager().createProject(projectName, owner, description,
                overrideProps, newProject.getMaintainModelType());
        createDefaultRules(projectName);
        logger.debug("New project created.");
        return createdProject;
    }

    private void createDefaultRules(String projectName) {
        // create default rules
        // frequency rule
        FavoriteRule.Condition freqCond = new FavoriteRule.Condition();
        freqCond.setRightThreshold("0.1");
        FavoriteRule freqRule = new FavoriteRule(Lists.newArrayList(freqCond), FavoriteRule.FREQUENCY_RULE_NAME, true);
        getFavoriteRuleManager(projectName).createRule(freqRule);
        // submitter rule
        FavoriteRule.Condition submitterCond = new FavoriteRule.Condition();
        submitterCond.setRightThreshold("ADMIN");
        FavoriteRule submitterRule = new FavoriteRule(Lists.newArrayList(submitterCond),
                FavoriteRule.SUBMITTER_RULE_NAME, true);
        getFavoriteRuleManager(projectName).createRule(submitterRule);
        // duration rule
        FavoriteRule.Condition durationCond = new FavoriteRule.Condition();
        durationCond.setLeftThreshold("0");
        durationCond.setRightThreshold("180");
        FavoriteRule durationRule = new FavoriteRule(Lists.newArrayList(durationCond), FavoriteRule.DURATION_RULE_NAME,
                false);
        getFavoriteRuleManager(projectName).createRule(durationRule);

        // create blacklist
        FavoriteRule blacklist = new FavoriteRule();
        blacklist.setName(FavoriteRule.BLACKLIST_NAME);
        getFavoriteRuleManager(projectName).createRule(blacklist);
    }

    public List<ProjectInstance> getReadableProjects(final String projectName) {
        List<ProjectInstance> projectInstances = new ArrayList<ProjectInstance>();
        if (!Strings.isEmpty(projectName)) {
            ProjectInstance projectInstance = getProjectManager().getProject(projectName);
            projectInstances.add(projectInstance);
        } else {
            projectInstances.addAll(getProjectManager().listAllProjects());
        }
        return projectInstances.stream()
                .filter(projectInstance -> aclEvaluate.hasProjectAdminPermission(projectInstance))
                .collect(Collectors.toList());
    }

    @Transaction
    public ProjectInstance updateProject(ProjectInstance newProject, ProjectInstance currentProject) {
        String newProjectName = newProject.getName();
        String newDescription = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();

        ProjectInstance updatedProject = getProjectManager().updateProject(currentProject, newProjectName,
                newDescription, overrideProps);

        logger.debug("Project updated.");
        return updatedProject;
    }

    @Transaction(project = 0)
    public void updateQueryAccelerateThresholdConfig(String project, Integer threshold, boolean autoApply,
            boolean batchEnabled) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put("kylin.favorite.query-accelerate-threshold", threshold.toString());
        overrideKylinProps.put("kylin.favorite.query-accelerate-threshold-batch-enable", batchEnabled + "");
        overrideKylinProps.put("kylin.favorite.query-accelerate-threshold-auto-apply", autoApply + "");
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    public FavoriteQueryThresholdResponse getQueryAccelerateThresholdConfig(String project) {
        val projectInstance = getProjectManager().getProject(project);
        val thresholdResponse = new FavoriteQueryThresholdResponse();
        val config = projectInstance.getConfig();
        thresholdResponse.setThreshold(config.getFavoriteQueryAccelerateThreshold());
        thresholdResponse.setBatchEnabled(config.getFavoriteQueryAccelerateThresholdBatchEnabled());
        thresholdResponse.setAutoApply(config.getFavoriteQueryAccelerateThresholdAutoApply());
        return thresholdResponse;
    }

    @Transaction(project = 0)
    public void updateMantainModelType(String project, String maintainModelType) {
        val projectManager = getProjectManager();
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.valueOf(maintainModelType));
        });
    }

    public StorageVolumeInfoResponse getStorageVolumeInfoResponse(String project) {
        val response = new StorageVolumeInfoResponse();
        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE, StorageInfoEnum.STORAGE_QUOTA,
                StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getConfig(), project);
        response.setGarbageStorageSize(storageVolumeInfo.getGarbageStorageSize());
        response.setStorageQuotaSize(storageVolumeInfo.getStorageQuotaSize());
        response.setTotalStorageSize(storageVolumeInfo.getTotalStorageSize());
        return response;
    }

    @Transaction(project = 0)
    public void cleanupProjectGarbageIndex(String project) throws IOException {
        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getConfig(), project);
        Map<String, Set<Long>> garbageIndexMap = storageVolumeInfo.getGarbageModelIndexMap();
        if (garbageIndexMap.size() > 0 && storageVolumeInfo.getThrowableMap().size() == 0) {
            cleanupGarbageIndex(project, garbageIndexMap);
        }
    }

    private void cleanupGarbageIndex(String project, Map<String, Set<Long>> garbageIndexMap) throws IOException {
        val indexPlanManager = getIndexPlanManager(project);
        for (Map.Entry<String, Set<Long>> entry : garbageIndexMap.entrySet()) {
            val modelId = entry.getKey();
            val cuboidLayoutIds = entry.getValue();
            val indexPlan = indexPlanManager.getIndexPlan(modelId);
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                copyForWrite.removeLayouts(cuboidLayoutIds, LayoutEntity::equals, true, false);
            });
        }
    }

    @Transaction(project = 0)
    public void updateStorageQuotaConfig(String project, long storageQuotaSize) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        long storageQuotaSizeGB = storageQuotaSize / (1024 * 1024 * 1024);
        overrideKylinProps.put("kylin.storage.quota-in-giga-bytes", String.valueOf(storageQuotaSizeGB));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    private void updateProjectOverrideKylinProps(String project, Map<String, String> overrideKylinProps) {
        val projectManager = getProjectManager();
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new BadRequestException(String.format("Project '%s' does not exist!", project));
        }
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });
    }

    @Transaction(project = 0)
    public void updateJobNotificationConfig(String project, JobNotificationConfigRequest jobNotificationConfigRequest) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put("kylin.job.notification-on-empty-data-load",
                String.valueOf(jobNotificationConfigRequest.isDataLoadEmptyNotificationEnabled()));
        overrideKylinProps.put("kylin.job.notification-on-job-error",
                String.valueOf(jobNotificationConfigRequest.isJobErrorNotificationEnabled()));
        overrideKylinProps.put("kylin.job.notification-admin-emails",
                convertToString(jobNotificationConfigRequest.getJobNotificationEmails()));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    private String convertToString(List<String> stringList) {
        var strValue = "";
        if (CollectionUtils.isEmpty(stringList)) {
            return strValue;
        }
        strValue = String.join(",", Sets.newHashSet(stringList));
        return strValue;
    }

    public ProjectConfigResponse getProjectConfig(String project) {
        val response = new ProjectConfigResponse();
        val projectInstance = getProjectManager().getProject(project);
        val config = projectInstance.getConfig();

        response.setProject(project);
        response.setDescription(projectInstance.getDescription());
        response.setMaintainModelType(projectInstance.getMaintainModelType());

        response.setStorageQuotaSize(config.getStorageQuotaSize());

        response.setPushDownEnabled(config.isPushDownEnabled());
        response.setPushDownRangeLimited(projectInstance.isPushDownRangeLimited());

        response.setAutoMergeEnabled(projectInstance.getSegmentConfig().getAutoMergeEnabled());
        response.setAutoMergeTimeRanges(projectInstance.getSegmentConfig().getAutoMergeTimeRanges());
        response.setVolatileRange(projectInstance.getSegmentConfig().getVolatileRange());
        response.setRetentionRange(projectInstance.getSegmentConfig().getRetentionRange());

        response.setFavoriteQueryThreshold(config.getFavoriteQueryAccelerateThreshold());
        response.setFavoriteQueryBatchEnabled(config.getFavoriteQueryAccelerateThresholdBatchEnabled());
        response.setFavoriteQueryAutoApply(config.getFavoriteQueryAccelerateThresholdAutoApply());

        response.setDataLoadEmptyNotificationEnabled(config.getJobDataLoadEmptyNotificationEnabled());
        response.setJobErrorNotificationEnabled(config.getJobErrorNotificationEnabled());
        response.setJobNotificationEmails(Lists.newArrayList(config.getAdminDls()));
        return response;
    }

    @Transaction(project = 0)
    public void updatePushDownConfig(String project, PushDownConfigRequest pushDownConfigRequest) {
        getProjectManager().updateProject(project, copyForWrite -> {
            val config = getConfig();
            if (pushDownConfigRequest.isPushDownEnabled()) {
                val pushDownRunner = config.getPushDownRunnerClassName();
                Preconditions.checkState(StringUtils.isNotBlank(pushDownRunner),
                        "There is no default PushDownRunner, please check kylin.query.pushdown.runner-class-name in kylin.properties.");
                copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown.runner-class-name", pushDownRunner);
            } else {
                copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown.runner-class-name", "");
            }
            copyForWrite.setPushDownRangeLimited(pushDownConfigRequest.isPushDownRangeLimited());
        });
    }

    @Transaction(project = 0)
    public void updateSegmentConfig(String project, SegmentConfigRequest segmentConfigRequest) {
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.getSegmentConfig().setAutoMergeEnabled(segmentConfigRequest.getAutoMergeEnabled());
            copyForWrite.getSegmentConfig().setAutoMergeTimeRanges(segmentConfigRequest.getAutoMergeTimeRanges());
            copyForWrite.getSegmentConfig().setVolatileRange(segmentConfigRequest.getVolatileRange());
            copyForWrite.getSegmentConfig().setRetentionRange(segmentConfigRequest.getRetentionRange());
        });
    }

    @Transaction(project = 0)
    public void updateProjectGeneralInfo(String project, ProjectGeneralInfoRequest projectGeneralInfoRequest) {
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.setDescription(projectGeneralInfoRequest.getDescription());
        });
    }

    @Transaction(project = 0)
    public void dropProject(String project) {
        val prjManager = getProjectManager();
        prjManager.forceDropProject(project);
        shutdownRelatedScheduler(project);
    }

    private void shutdownRelatedScheduler(String project) {
        val eventOrchestratorManager = EventOrchestratorManager.getInstance(getConfig());
        eventOrchestratorManager.shutDownByProject(project);
        NDefaultScheduler.shutDownByProject(project);
        NFavoriteScheduler.shutDownByProject(project);
    }

    public String backupProject(String project) throws Exception {
        return metadataBackupService.backupProject(project);
    }

    public void clearManagerCache(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        config.clearManagersByProject(project);
        config.clearManagersByClz(NProjectManager.class);
    }
}
