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

import static org.apache.kylin.common.exception.ServerErrorCode.DATABASE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_EMAIL;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_TYPE_MISMATCH;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionLock;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.config.initialize.ProjectDropListener;
import io.kyligence.kap.rest.request.ComputedColumnConfigRequest;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectKerberosInfoRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.PushDownProjectConfigRequest;
import io.kyligence.kap.rest.request.SCD2ConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import io.kyligence.kap.rest.response.ProjectConfigResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.security.KerberosLoginManager;
import io.kyligence.kap.rest.service.task.QueryHistoryAccelerateScheduler;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.source.file.CredentialOperator;
import io.kyligence.kap.tool.garbage.GarbageCleaner;
import lombok.val;

@Component("projectService")
public class ProjectService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private MetadataBackupService metadataBackupService;

    @Autowired
    private AsyncTaskService asyncTaskService;

    @Autowired
    private AccessService accessService;

    private static final String DEFAULT_VAL = "default";

    private static final String SPARK_YARN_QUEUE = "kylin.engine.spark-conf.spark.yarn.queue";

    private static final List<String> favoriteRuleNames = Lists.newArrayList(FavoriteRule.COUNT_RULE_NAME,
            FavoriteRule.FREQUENCY_RULE_NAME, FavoriteRule.DURATION_RULE_NAME, FavoriteRule.SUBMITTER_RULE_NAME,
            FavoriteRule.SUBMITTER_GROUP_RULE_NAME, FavoriteRule.REC_SELECT_RULE_NAME);

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = -1)
    public ProjectInstance createProject(String projectName, ProjectInstance newProject) {
        Message msg = MsgPicker.getMsg();
        String description = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();
        if (overrideProps == null) {
            overrideProps = Maps.newLinkedHashMap();
        }
        if (newProject.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN) {
            overrideProps.put("kylin.metadata.semi-automatic-mode", "true");
            overrideProps.put(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF, "true");
        } else {
            overrideProps.put(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF, "false");
        }
        ProjectInstance currentProject = getProjectManager().getProject(projectName);
        if (currentProject != null) {
            throw new KylinException(DUPLICATE_PROJECT_NAME,
                    String.format(msg.getPROJECT_ALREADY_EXIST(), projectName));
        }
        final String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        ProjectInstance createdProject = getProjectManager().createProject(projectName, owner, description,
                overrideProps, newProject.getMaintainModelType());
        logger.debug("New project created.");
        return createdProject;
    }

    public List<ProjectInstance> getReadableProjects() {
        return getProjectsFilterByExactMatchAndPermission(null, false, AclPermissionEnum.READ);
    }

    public List<ProjectInstance> getAdminProjects() {
        return getProjectsFilterByExactMatchAndPermission(null, false, AclPermissionEnum.ADMINISTRATION);
    }

    public List<ProjectInstance> getReadableProjects(final String projectName, boolean exactMatch) {
        return getProjectsFilterByExactMatchAndPermission(projectName, exactMatch, AclPermissionEnum.READ);
    }

    public List<ProjectInstance> getProjectsFilterByExactMatchAndPermission(final String projectName,
            boolean exactMatch, AclPermissionEnum permission) {
        Predicate<ProjectInstance> filter;
        switch (permission) {
        case READ:
            filter = projectInstance -> aclEvaluate.hasProjectReadPermission(projectInstance);
            break;
        case OPERATION:
            filter = projectInstance -> aclEvaluate.hasProjectOperationPermission(projectInstance);
            break;
        case MANAGEMENT:
            filter = projectInstance -> aclEvaluate.hasProjectWritePermission(projectInstance);
            break;
        case ADMINISTRATION:
            filter = projectInstance -> aclEvaluate.hasProjectAdminPermission(projectInstance);
            break;
        default:
            throw new KylinException(PERMISSION_DENIED, "Operation failed, unknown permission:" + permission);
        }
        if (StringUtils.isNotBlank(projectName)) {
            Predicate<ProjectInstance> exactMatchFilter = projectInstance -> (exactMatch
                    && projectInstance.getName().equals(projectName))
                    || (!exactMatch && projectInstance.getName().toUpperCase().contains(projectName.toUpperCase()));
            filter = filter.and(exactMatchFilter);
        }
        return getProjectsWithFilter(filter);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateQueryAccelerateThresholdConfig(String project, Integer threshold, boolean tipsEnabled) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        if (threshold != null) {
            if (threshold <= 0) {
                throw new KylinException(INVALID_PARAMETER,
                        "No valid value for 'threshold'. Please set an integer 'x' "
                                + "greater than 0 to 'threshold'. The system will notify you whenever there "
                                + "are more then 'x' queries waiting to accelerate.");
            }
            overrideKylinProps.put("kylin.favorite.query-accelerate-threshold", String.valueOf(threshold));
        }
        overrideKylinProps.put("kylin.favorite.query-accelerate-tips-enable", String.valueOf(tipsEnabled));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public FavoriteQueryThresholdResponse getQueryAccelerateThresholdConfig(String project) {
        val projectInstance = getProjectManager().getProject(project);
        val thresholdResponse = new FavoriteQueryThresholdResponse();
        val config = projectInstance.getConfig();
        thresholdResponse.setThreshold(config.getFavoriteQueryAccelerateThreshold());
        thresholdResponse.setTipsEnabled(config.getFavoriteQueryAccelerateTipsEnabled());
        return thresholdResponse;
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

    public void garbageCleanup() {
        String oldThreadName = Thread.currentThread().getName();

        try {
            Thread.currentThread().setName("GarbageCleanupWorker");
            // clean up acl
            cleanupAcl();
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            val epochMgr = EpochManager.getInstance(config);
            for (ProjectInstance project : projectManager.listAllProjects()) {
                if (!config.isUTEnv() && !epochMgr.checkEpochOwner(project.getName()))
                    continue;
                logger.info("Start to cleanup garbage  for project<{}>", project.getName());
                try {
                    updateProjectRegularRule(project.getName());
                    GarbageCleaner.cleanupMetadataAtScheduledTime(project.getName());
                } catch (Exception e) {
                    logger.warn("clean project<" + project.getName() + "> failed", e);
                }
                logger.info("Garbage cleanup for project<{}> finished", project.getName());
            }
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }

    }

    private void cleanupAcl() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val prjManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            List<String> prjects = prjManager.listAllProjects().stream().map(ProjectInstance::getUuid)
                    .collect(Collectors.toList());
            val aclManager = AclManager.getInstance(KylinConfig.getInstanceFromEnv());
            for (val acl : aclManager.listAll()) {
                String id = acl.getDomainObjectInfo().getId();
                if (!prjects.contains(id)) {
                    aclManager.delete(id);
                }
            }
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public void cleanupGarbage(String project) throws Exception {
        updateProjectRegularRule(project);
        GarbageCleaner.cleanupMetadataManually(project);
        asyncTaskService.cleanupStorage();
    }

    public void updateProjectRegularRule(String project) {
        QueryHistoryAccelerateScheduler scheduler = QueryHistoryAccelerateScheduler.getInstance(project);
        if (scheduler.hasStarted()) {
            Future future = scheduler.scheduleImmediately();
            try {
                future.get();
            } catch (Exception e) {
                logger.error("msg", e);
            }
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateStorageQuotaConfig(String project, long storageQuotaSize) {
        if (storageQuotaSize < FileUtils.ONE_TB) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid storage quota size, Please set an integer greater than or equal to 1TB "
                            + "to 'storage_quota_size', unit byte.");
        }
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        double storageQuotaSizeGB = 1.0 * storageQuotaSize / (FileUtils.ONE_GB);
        overrideKylinProps.put("kylin.storage.quota-in-giga-bytes", Double.toString(storageQuotaSizeGB));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    private void updateProjectOverrideKylinProps(String project, Map<String, String> overrideKylinProps) {
        val projectManager = getProjectManager();
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateFileSourceCredential(String project, CredentialOperator credentialOperator) {
        Map<String, String> overrideKylinProps = Maps.newLinkedHashMap();
        overrideKylinProps.put("kylin.source.credential.type", credentialOperator.getCredential().getType());
        overrideKylinProps.put("kylin.source.credential.value", credentialOperator.encode());
        overrideKylinProps.put("kylin.source.default", String.valueOf(ISourceAware.ID_FILE));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @Transaction(project = 0)
    public void updateJobNotificationConfig(String project, JobNotificationConfigRequest jobNotificationConfigRequest) {
        aclEvaluate.checkProjectAdminPermission(project);
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put("kylin.job.notification-on-empty-data-load",
                String.valueOf(jobNotificationConfigRequest.getDataLoadEmptyNotificationEnabled()));
        overrideKylinProps.put("kylin.job.notification-on-job-error",
                String.valueOf(jobNotificationConfigRequest.getJobErrorNotificationEnabled()));
        overrideKylinProps.put("kylin.job.notification-admin-emails",
                convertToString(jobNotificationConfigRequest.getJobNotificationEmails()));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void updateYarnQueue(String project, String queueName) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put(SPARK_YARN_QUEUE, queueName);
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    private String convertToString(List<String> stringList) {
        if (CollectionUtils.isEmpty(stringList)) {
            throw new KylinException(EMPTY_EMAIL, "Please enter at least one email address.");
        }
        Set<String> notEmails = Sets.newHashSet();
        for (String email : Sets.newHashSet(stringList)) {
            Pattern pattern = Pattern.compile("^[a-zA-Z0-9_.-]+@[a-zA-Z0-9-]+(\\.[a-zA-Z0-9-]+)*\\.[a-zA-Z0-9]{2,6}$");
            Matcher matcher = pattern.matcher(email);
            if (!matcher.find()) {
                notEmails.add(email);
            }
        }
        if (!notEmails.isEmpty()) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value " + notEmails + " for 'job_notification_email'. Please enter valid email address.");
        }
        return String.join(",", Sets.newHashSet(stringList));
    }

    public ProjectConfigResponse getProjectConfig0(String project) {
        val response = new ProjectConfigResponse();
        val projectInstance = getProjectManager().getProject(project);
        val config = projectInstance.getConfig();

        response.setProject(project);
        response.setDescription(projectInstance.getDescription());
        response.setMaintainModelType(projectInstance.getMaintainModelType());
        response.setDefaultDatabase(projectInstance.getDefaultDatabase());
        response.setSemiAutomaticMode(config.isSemiAutoMode());

        response.setStorageQuotaSize(config.getStorageQuotaSize());

        response.setPushDownEnabled(config.isPushDownEnabled());
        response.setRunnerClassName(config.getPushDownRunnerClassName());
        response.setConverterClassNames(String.join(",", config.getPushDownConverterClassNames()));

        response.setAutoMergeEnabled(projectInstance.getSegmentConfig().getAutoMergeEnabled());
        response.setAutoMergeTimeRanges(projectInstance.getSegmentConfig().getAutoMergeTimeRanges());
        response.setVolatileRange(projectInstance.getSegmentConfig().getVolatileRange());
        response.setRetentionRange(projectInstance.getSegmentConfig().getRetentionRange());

        response.setFavoriteQueryThreshold(config.getFavoriteQueryAccelerateThreshold());
        response.setFavoriteQueryTipsEnabled(config.getFavoriteQueryAccelerateTipsEnabled());

        response.setDataLoadEmptyNotificationEnabled(config.getJobDataLoadEmptyNotificationEnabled());
        response.setJobErrorNotificationEnabled(config.getJobErrorNotificationEnabled());
        response.setJobNotificationEmails(Lists.newArrayList(config.getAdminDls()));

        response.setFrequencyTimeWindow(config.getFrequencyTimeWindowInDays());

        response.setLowFrequencyThreshold(config.getLowFrequencyThreshold());

        response.setYarnQueue(config.getOptional(SPARK_YARN_QUEUE, DEFAULT_VAL));

        response.setExposeComputedColumn(config.exposeComputedColumn());

        response.setKerberosProjectLevelEnabled(config.getKerberosProjectLevelEnable());

        response.setPrincipal(projectInstance.getPrincipal());
        // return favorite rules
        response.setFavoriteRules(getFavoriteRules(project));

        response.setScd2Enabled(config.isQueryNonEquiJoinModelEnabled());

        return response;
    }

    public ProjectConfigResponse getProjectConfig(String project) {
        aclEvaluate.checkProjectReadPermission(project);
        return getProjectConfig0(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateShardNumConfig(String project, ShardNumConfigRequest req) {
        getProjectManager().updateProject(project, copyForWrite -> {
            try {
                copyForWrite.getOverrideKylinProps().put("kylin.engine.shard-num-json",
                        JsonUtil.writeValueAsString(req.getColToNum()));
            } catch (JsonProcessingException e) {
                logger.error("Can not write obj to json.", e);
            }
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public String getShardNumConfig(String project) {
        return getProjectManager().getProject(project).getConfig().getExtendedOverrides()
                .getOrDefault("kylin.engine.shard-num-json", "");
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updatePushDownConfig(String project, PushDownConfigRequest pushDownConfigRequest) {
        getProjectManager().updateProject(project, copyForWrite -> {
            if (Boolean.TRUE.equals(pushDownConfigRequest.getPushDownEnabled())) {
                String runnerClassName = copyForWrite.getConfig().getPushDownRunnerClassName();
                if (StringUtils.isEmpty(runnerClassName)) {
                    val defaultPushDownRunner = getConfig().getPushDownRunnerClassNameWithDefaultValue();
                    copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown.runner-class-name",
                            defaultPushDownRunner);
                }
                copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown-enabled", "true");
            } else {
                copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown-enabled", "false");
            }
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateSCD2Config(String project, SCD2ConfigRequest scd2ConfigRequest, ModelService modelService) {
        getProjectManager().updateProject(project, copyForWrite -> {
            if (Boolean.TRUE.equals(scd2ConfigRequest.getScd2Enabled())) {
                copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "true");
            } else {
                copyForWrite.getOverrideKylinProps().put("kylin.query.non-equi-join-model-enabled", "false");
                modelService.offlineSCD2ModelsInProjectById(project);
            }
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updatePushDownProjectConfig(String project, PushDownProjectConfigRequest pushDownProjectConfigRequest) {
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown.runner-class-name",
                    pushDownProjectConfigRequest.getRunnerClassName());
            copyForWrite.getOverrideKylinProps().put("kylin.query.pushdown.converter-class-names",
                    pushDownProjectConfigRequest.getConverterClassNames());
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateComputedColumnConfig(String project, ComputedColumnConfigRequest computedColumnConfigRequest) {
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF,
                    String.valueOf(computedColumnConfigRequest.getExposeComputedColumn()));
        });
    }

    @Transaction(project = 0)
    public void updateSegmentConfig(String project, SegmentConfigRequest segmentConfigRequest) {
        aclEvaluate.checkProjectAdminPermission(project);
        //api send volatileRangeEnabled = false but finally it is reset to true
        segmentConfigRequest.getVolatileRange().setVolatileRangeEnabled(true);
        if (segmentConfigRequest.getVolatileRange().getVolatileRangeNumber() < 0) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value. Please set an integer 'x' to "
                            + "'volatile_range_number'. The 'Auto-Merge' will not merge latest 'x' "
                            + "period(day/week/month/etc..) segments.");
        }
        if (segmentConfigRequest.getRetentionRange().getRetentionRangeNumber() < 0) {
            throw new KylinException(INVALID_PARAMETER, "No valid value for 'retention_range_number'."
                    + " Please set an integer 'x' to specify the retention threshold. The system will "
                    + "only retain the segments in the retention threshold (x years before the last data time). ");
        }
        if (segmentConfigRequest.getAutoMergeTimeRanges().isEmpty()) {
            throw new KylinException(INVALID_PARAMETER, "No valid value for 'auto_merge_time_ranges'. Please set "
                    + "{'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'} to specify the period of auto-merge. ");
        }
        segmentConfigRequest.getRetentionRange().setRetentionRangeType(segmentConfigRequest.getAutoMergeTimeRanges()
                .stream().max(Comparator.comparing(AutoMergeTimeEnum::ordinal)).orElse(AutoMergeTimeEnum.YEAR));
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.getSegmentConfig().setAutoMergeEnabled(segmentConfigRequest.getAutoMergeEnabled());
            copyForWrite.getSegmentConfig().setAutoMergeTimeRanges(segmentConfigRequest.getAutoMergeTimeRanges());
            copyForWrite.getSegmentConfig().setVolatileRange(segmentConfigRequest.getVolatileRange());
            copyForWrite.getSegmentConfig().setRetentionRange(segmentConfigRequest.getRetentionRange());
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateProjectGeneralInfo(String project, ProjectGeneralInfoRequest projectGeneralInfoRequest) {
        if (getProjectManager().getProject(project).isSmartMode()) {
            projectGeneralInfoRequest.setSemiAutoMode(false);
        }
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.setDescription(projectGeneralInfoRequest.getDescription());
            copyForWrite.getOverrideKylinProps().put("kylin.metadata.semi-automatic-mode",
                    String.valueOf(projectGeneralInfoRequest.isSemiAutoMode()));
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateProjectKerberosInfo(String project, ProjectKerberosInfoRequest projectKerberosInfoRequest)
            throws Exception {
        KerberosLoginManager.getInstance().checkAndReplaceProjectKerberosInfo(project,
                projectKerberosInfoRequest.getPrincipal());
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.setPrincipal(projectKerberosInfoRequest.getPrincipal());
            copyForWrite.setKeytab(projectKerberosInfoRequest.getKeytab());
        });

        backupAndDeleteKeytab(projectKerberosInfoRequest.getPrincipal());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void dropProject(String project) {
        val prjManager = getProjectManager();
        prjManager.forceDropProject(project);
        UnitOfWork.get().doAfterUnit(() -> new ProjectDropListener().onDelete(project));
        SchedulerEventBusFactory.getInstance(KylinConfig.getInstanceFromEnv()).post(new SourceUsageUpdateNotifier());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateDefaultDatabase(String project, String defaultDatabase) {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(defaultDatabase);
        String uppderDB = defaultDatabase.toUpperCase();

        val prjManager = getProjectManager();
        val tableManager = getTableManager(project);
        if (ProjectInstance.DEFAULT_DATABASE.equals(uppderDB) || tableManager.listDatabases().contains(uppderDB)) {
            final ProjectInstance projectInstance = prjManager.getProject(project);
            if (uppderDB.equals(projectInstance.getDefaultDatabase())) {
                return;
            }
            projectInstance.setDefaultDatabase(uppderDB);
            prjManager.updateProject(projectInstance);
        } else {
            throw new KylinException(DATABASE_NOT_EXIST,
                    String.format(MsgPicker.getMsg().getDATABASE_NOT_EXIST(), defaultDatabase));
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public String backupProject(String project) throws Exception {
        return metadataBackupService.backupProject(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public void clearManagerCache(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        config.clearManagersByProject(project);
        config.clearManagersByClz(NProjectManager.class);
    }

    @Transaction(project = 0)
    public void setDataSourceType(String project, String sourceType) {
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.source.default", sourceType);
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateGarbageCleanupConfig(String project, GarbageCleanUpConfigRequest garbageCleanUpConfigRequest) {
        if (garbageCleanUpConfigRequest.getLowFrequencyThreshold() < 0L) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value for 'low_frequency_threshold'. Please "
                            + "set an integer 'x' greater than or equal to 0 to specify the low usage storage "
                            + "calculation time. When index usage is lower than 'x' times, it would be regarded "
                            + "as low usage storage.");
        }
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put("kylin.cube.low-frequency-threshold",
                String.valueOf(garbageCleanUpConfigRequest.getLowFrequencyThreshold()));
        overrideKylinProps.put("kylin.cube.frequency-time-window",
                String.valueOf(garbageCleanUpConfigRequest.getFrequencyTimeWindow()));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    public Map<String, Object> getFavoriteRules(String project) {
        Map<String, Object> result = Maps.newHashMap();

        for (String ruleName : favoriteRuleNames) {
            getSingleRule(project, ruleName, result);
        }

        return result;
    }

    private void getSingleRule(String project, String ruleName, Map<String, Object> result) {
        FavoriteRule rule = getFavoriteRule(project, ruleName);
        List<FavoriteRule.Condition> conds = (List<FavoriteRule.Condition>) (List<?>) rule.getConds();

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            result.put("freq_enable", rule.isEnabled());
            String frequency = CollectionUtils.isEmpty(conds) ? null : conds.get(0).getRightThreshold();
            result.put("freq_value", StringUtils.isEmpty(frequency) ? null : Float.valueOf(frequency));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            result.put("count_enable", rule.isEnabled());
            String count = conds.get(0).getRightThreshold();
            result.put("count_value", StringUtils.isEmpty(count) ? null : Float.valueOf(count));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            List<String> users = Lists.newArrayList();
            conds.forEach(cond -> users.add(cond.getRightThreshold()));
            result.put("submitter_enable", rule.isEnabled());
            result.put("users", users);
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            List<String> userGroups = Lists.newArrayList();
            conds.forEach(cond -> userGroups.add(cond.getRightThreshold()));
            result.put("user_groups", userGroups);
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            result.put("duration_enable", rule.isEnabled());
            String minDuration = CollectionUtils.isEmpty(conds) ? null : conds.get(0).getLeftThreshold();
            String maxDuration = CollectionUtils.isEmpty(conds) ? null : conds.get(0).getRightThreshold();
            result.put("min_duration", StringUtils.isEmpty(minDuration) ? null : Long.valueOf(minDuration));
            result.put("max_duration", StringUtils.isEmpty(maxDuration) ? null : Long.valueOf(maxDuration));
            break;
        case FavoriteRule.REC_SELECT_RULE_NAME:
            result.put("recommendation_enable", rule.isEnabled());
            String upperBound = conds.get(0).getRightThreshold();
            result.put("recommendations_value", StringUtils.isEmpty(upperBound) ? null : Long.valueOf(upperBound));
            break;
        default:
            break;
        }
    }

    private FavoriteRule getFavoriteRule(String project, String ruleName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        Preconditions.checkArgument(StringUtils.isNotEmpty(ruleName));

        return FavoriteRule.getDefaultRule(getFavoriteRuleManager(project).getByName(ruleName), ruleName);
    }

    @Transaction(project = 0)
    public void updateRegularRule(String project, FavoriteRuleUpdateRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        favoriteRuleNames.forEach(ruleName -> updateSingleRule(project, ruleName, request));
    }

    private void updateSingleRule(String project, String ruleName, FavoriteRuleUpdateRequest request) {
        List<FavoriteRule.Condition> conds = Lists.newArrayList();
        boolean isEnabled = false;

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            isEnabled = request.isFreqEnable();
            conds.add(new FavoriteRule.Condition(null, request.getFreqValue()));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            isEnabled = request.isCountEnable();
            conds.add(new FavoriteRule.Condition(null, request.getCountValue()));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUsers()))
                request.getUsers().forEach(user -> conds.add(new FavoriteRule.Condition(null, user)));
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUserGroups()))
                request.getUserGroups().forEach(userGroup -> conds.add(new FavoriteRule.Condition(null, userGroup)));
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            isEnabled = request.isDurationEnable();
            conds.add(new FavoriteRule.Condition(request.getMinDuration(), request.getMaxDuration()));
            break;
        case FavoriteRule.REC_SELECT_RULE_NAME:
            isEnabled = request.isRecommendationEnable();
            conds.add(new FavoriteRule.Condition(null, request.getRecommendationsValue()));
            break;
        default:
            break;
        }

        getFavoriteRuleManager(project).updateRule(conds, isEnabled, ruleName);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public ProjectConfigResponse resetProjectConfig(String project, String resetItem) {
        Preconditions.checkNotNull(resetItem);
        switch (resetItem) {
        case "job_notification_config":
            resetJobNotificationConfig(project);
            break;
        case "query_accelerate_threshold":
            resetQueryAccelerateThreshold(project);
            break;
        case "garbage_cleanup_config":
            resetGarbageCleanupConfig(project);
            break;
        case "segment_config":
            resetSegmentConfig(project);
            break;
        case "kerberos_project_level_config":
            resetProjectKerberosConfig(project);
            break;
        case "storage_quota_config":
            resetProjectStorageQuotaConfig(project);
            break;
        case "favorite_rule_config":
            resetProjectRecommendationConfig(project);
            break;
        default:
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value for 'reset_item'. Please enter a project setting "
                            + "type which needs to be reset {'job_notification_config'，"
                            + "'query_accelerate_threshold'，'garbage_cleanup_config'，'segment_config', 'storage_quota_config'} to 'reset_item'.");
        }
        return getProjectConfig(project);
    }

    @Transaction(project = 0)
    public void updateProjectOwner(String project, OwnerChangeRequest ownerChangeRequest) {
        try {
            aclEvaluate.checkIsGlobalAdmin();
            checkTargetOwnerPermission(project, ownerChangeRequest.getOwner());
        } catch (AccessDeniedException e) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getPROJECT_CHANGE_PERMISSION());
        } catch (IOException e) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getOWNER_CHANGE_ERROR());
        }

        getProjectManager().updateProject(project,
                copyForWrite -> copyForWrite.setOwner(ownerChangeRequest.getOwner()));
    }

    private void checkTargetOwnerPermission(String project, String owner) throws IOException {
        Set<String> projectAdminUsers = accessService.getProjectAdminUsers(project);
        projectAdminUsers.remove(getProjectManager().getProject(project).getOwner());
        if (CollectionUtils.isEmpty(projectAdminUsers) || !projectAdminUsers.contains(owner)) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(PERMISSION_DENIED, msg.getPROJECT_OWNER_CHANGE_INVALID_USER());
        }
    }

    public boolean isProjectWriteLocked(String project) {
        return TransactionLock.isWriteLocked(project);
    }

    private void resetJobNotificationConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.job.notification-on-empty-data-load");
        toBeRemovedProps.add("kylin.job.notification-on-job-error");
        toBeRemovedProps.add("kylin.job.notification-admin-emails");
        removeProjectOveridedProps(project, toBeRemovedProps);
    }

    private void resetQueryAccelerateThreshold(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.favorite.query-accelerate-threshold");
        toBeRemovedProps.add("kylin.favorite.query-accelerate-tips-enable");
        removeProjectOveridedProps(project, toBeRemovedProps);
    }

    private void resetProjectRecommendationConfig(String project) {
        val countList = Lists.newArrayList(FavoriteRule.getDefaultCondition(FavoriteRule.COUNT_RULE_NAME));
        val submitterList = Lists.newArrayList(FavoriteRule.getDefaultCondition(FavoriteRule.SUBMITTER_RULE_NAME));
        val groupList = Lists.newArrayList(FavoriteRule.getDefaultCondition(FavoriteRule.SUBMITTER_GROUP_RULE_NAME));
        val recList = Lists.newArrayList(FavoriteRule.getDefaultCondition(FavoriteRule.REC_SELECT_RULE_NAME));

        getFavoriteRuleManager(project).updateRule(Lists.newArrayList(), false, FavoriteRule.FREQUENCY_RULE_NAME);
        getFavoriteRuleManager(project).updateRule(countList, true, FavoriteRule.COUNT_RULE_NAME);
        getFavoriteRuleManager(project).updateRule(Lists.newArrayList(), false, FavoriteRule.DURATION_RULE_NAME);
        getFavoriteRuleManager(project).updateRule(submitterList, true, FavoriteRule.SUBMITTER_RULE_NAME);
        getFavoriteRuleManager(project).updateRule(groupList, true, FavoriteRule.SUBMITTER_GROUP_RULE_NAME);
        getFavoriteRuleManager(project).updateRule(recList, true, FavoriteRule.REC_SELECT_RULE_NAME);
    }

    private void resetGarbageCleanupConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.cube.low-frequency-threshold");
        toBeRemovedProps.add("kylin.cube.frequency-time-window");
        removeProjectOveridedProps(project, toBeRemovedProps);
    }

    private void resetSegmentConfig(String project) {
        getProjectManager().updateProject(project, copyForWrite -> {
            val projectInstance = new ProjectInstance();
            copyForWrite.getSegmentConfig()
                    .setAutoMergeEnabled(projectInstance.getSegmentConfig().getAutoMergeEnabled());
            copyForWrite.getSegmentConfig()
                    .setAutoMergeTimeRanges(projectInstance.getSegmentConfig().getAutoMergeTimeRanges());
            copyForWrite.getSegmentConfig().setVolatileRange(projectInstance.getSegmentConfig().getVolatileRange());
            copyForWrite.getSegmentConfig().setRetentionRange(projectInstance.getSegmentConfig().getRetentionRange());
        });
    }

    private void removeProjectOveridedProps(String project, Set<String> toBeRemovedProps) {
        val projectManager = getProjectManager();
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        projectManager.updateProject(project, copyForWrite -> {
            toBeRemovedProps.forEach(copyForWrite.getOverrideKylinProps()::remove);
        });
    }

    private void resetProjectKerberosConfig(String project) {
        val projectManager = getProjectManager();
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, String.format("Project '%s' does not exist!", project));
        }
        getProjectManager().updateProject(project, copyForWrite -> {
            copyForWrite.setKeytab(null);
            copyForWrite.setPrincipal(null);
        });
    }

    private void resetProjectStorageQuotaConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.storage.quota-in-giga-bytes");
        removeProjectOveridedProps(project, toBeRemovedProps);
    }

    private List<ProjectInstance> getProjectsWithFilter(Predicate<ProjectInstance> filter) {
        val allProjects = getProjectManager().listAllProjects();
        return allProjects.stream().filter(filter).collect(Collectors.toList());
    }

    public File backupAndDeleteKeytab(String principal) throws Exception {
        String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
        File kTempFile = new File(kylinConfHome, principal + KerberosLoginManager.TMP_KEYTAB_SUFFIX);
        File kFile = new File(kylinConfHome, principal + KerberosLoginManager.KEYTAB_SUFFIX);
        if (kTempFile.exists()) {
            FileUtils.copyFile(kTempFile, kFile);
            FileUtils.forceDelete(kTempFile);
        }
        return kFile;
    }

    public File generateTempKeytab(String principal, MultipartFile keytabFile) throws Exception {
        Message msg = MsgPicker.getMsg();
        if (null == principal || principal.isEmpty()) {
            throw new KylinException(EMPTY_PARAMETER, msg.getPRINCIPAL_EMPTY());
        }
        if (!keytabFile.getOriginalFilename().endsWith(".keytab")) {
            throw new KylinException(FILE_TYPE_MISMATCH, msg.getKEYTAB_FILE_TYPE_MISMATCH());
        }
        String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
        File kFile = new File(kylinConfHome, principal + KerberosLoginManager.TMP_KEYTAB_SUFFIX);
        FileUtils.copyInputStreamToFile(keytabFile.getInputStream(), kFile);
        return kFile;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void updateProjectConfig(String project, Map<String, String> overrides) {
        if (MapUtils.isEmpty(overrides)) {
            throw new KylinException(EMPTY_PARAMETER, "config map is required");
        }
        updateProjectOverrideKylinProps(project, overrides);
    }
}
