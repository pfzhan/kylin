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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NAME_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.validation.Valid;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.UserProjectPermissionResponse;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.common.util.FileUtils;
import io.kyligence.kap.rest.request.ComputedColumnConfigRequest;
import io.kyligence.kap.rest.request.DataSourceTypeRequest;
import io.kyligence.kap.rest.request.DefaultDatabaseRequest;
import io.kyligence.kap.rest.request.FavoriteQueryThresholdRequest;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JdbcRequest;
import io.kyligence.kap.rest.request.JdbcSourceInfoRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.MultiPartitionConfigRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.ProjectConfigRequest;
import io.kyligence.kap.rest.request.ProjectConfigResetRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectKerberosInfoRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.PushDownProjectConfigRequest;
import io.kyligence.kap.rest.request.SCD2ConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.request.SnapshotConfigRequest;
import io.kyligence.kap.rest.request.StorageQuotaRequest;
import io.kyligence.kap.rest.request.YarnQueueRequest;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import io.kyligence.kap.rest.response.ProjectConfigResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.EpochService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ProjectService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/projects", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NProjectController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NProjectController.class);

    private static final char[] VALID_PROJECT_NAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("epochService")
    private EpochService epochService;

    @ApiOperation(value = "getProjects", tags = {
            "SM" }, notes = "Update Param: page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<UserProjectPermissionResponse>>> getProjects(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer size,
            @RequestParam(value = "exact", required = false, defaultValue = "false") boolean exactMatch,
            @RequestParam(value = "permission", required = false, defaultValue = "READ") String permission)
            throws IOException {
        if (Objects.isNull(AclPermissionFactory.getPermission(permission))) {
            throw new KylinException(PERMISSION_DENIED, "Operation failed, unknown permission:" + permission);
        }
        List<UserProjectPermissionResponse> projects = projectService
                .getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(project, exactMatch,
                        AclPermissionEnum.valueOf(permission));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(projects, offset, size), "");
    }

    @ApiOperation(value = "deleteProjects", tags = { "SM" })
    @DeleteMapping(value = "/{project:.+}")
    @ResponseBody
    public EnvelopeResponse<String> dropProject(@PathVariable("project") String project) {
        projectService.dropProject(project);
        projectService.clearManagerCache(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "backupProject", tags = { "SM" }, notes = "Update URL, {project}")
    @PostMapping(value = "/{project:.+}/backup")
    @ResponseBody
    public EnvelopeResponse<String> backupProject(@PathVariable("project") String project) throws Exception {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectService.backupProject(project), "");
    }

    @ApiOperation(value = "saveProject", tags = {
            "SM" }, notes = "Update Param: former_project_name, project_desc_data")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<ProjectInstance> saveProject(@Valid @RequestBody ProjectRequest projectRequest) {
        checkRequiredArg("maintain_model_type", projectRequest.getMaintainModelType());

        ProjectInstance projectDesc = new ProjectInstance();
        BeanUtils.copyProperties(projectRequest, projectDesc);
        checkRequiredArg("name", projectRequest.getName());
        if (StringUtils.isEmpty(projectRequest.getName())
                || !StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECT_NAME)) {
            throw new KylinException(INVALID_PROJECT_NAME, MsgPicker.getMsg().getINVALID_PROJECT_NAME());
        }
        if (projectRequest.getName().length() > MAX_NAME_LENGTH) {
            throw new KylinException(PROJECT_NAME_ILLEGAL, MsgPicker.getMsg().getPROJECT_NAME_IS_ILLEGAL());
        }

        ProjectInstance createdProj = projectService.createProject(projectDesc.getName(), projectDesc);
        try {
            epochService.updateEpoch(Collections.singletonList(projectDesc.getName()), false, false);
        } catch (Exception e) {
            logger.warn("Transfer update epoch {} request failed, wait for schedule worker to update epoch.",
                    projectDesc.getName(), e);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, createdProj, "");
    }

    @ApiOperation(value = "updateDefaultDatabase", tags = {
            "SM" }, notes = "Add URL: {project}; Update Param: default_database;")
    @PutMapping(value = "/{project:.+}/default_database")
    @ResponseBody
    public EnvelopeResponse<String> updateDefaultDatabase(@PathVariable("project") String project,
            @RequestBody DefaultDatabaseRequest defaultDatabaseRequest) {
        checkRequiredArg("default_database", defaultDatabaseRequest.getDefaultDatabase());

        projectService.updateDefaultDatabase(project, defaultDatabaseRequest.getDefaultDatabase());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateQueryAccelerateThresholdConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/query_accelerate_threshold")
    @ResponseBody
    public EnvelopeResponse<String> updateQueryAccelerateThresholdConfig(@PathVariable("project") String project,
            @RequestBody FavoriteQueryThresholdRequest favoriteQueryThresholdRequest) {
        checkRequiredArg("tips_enabled", favoriteQueryThresholdRequest.getTipsEnabled());
        if (Boolean.TRUE.equals(favoriteQueryThresholdRequest.getTipsEnabled())) {
            checkRequiredArg("threshold", favoriteQueryThresholdRequest.getThreshold());
        }
        projectService.updateQueryAccelerateThresholdConfig(project, favoriteQueryThresholdRequest.getThreshold(),
                favoriteQueryThresholdRequest.getTipsEnabled());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getQueryAccelerateThresholdConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @GetMapping(value = "/{project:.+}/query_accelerate_threshold")
    @ResponseBody
    public EnvelopeResponse<FavoriteQueryThresholdResponse> getQueryAccelerateThresholdConfig(
            @PathVariable(value = "project") String project) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                projectService.getQueryAccelerateThresholdConfig(project), "");
    }

    @ApiOperation(value = "getStorageVolumeInfo", tags = { "SM" }, notes = "Add URL: {project}; ")
    @GetMapping(value = "/{project:.+}/storage_volume_info")
    @ResponseBody
    public EnvelopeResponse<StorageVolumeInfoResponse> getStorageVolumeInfo(
            @PathVariable(value = "project") String project) {
        aclEvaluate.checkProjectReadPermission(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectService.getStorageVolumeInfoResponse(project),
                "");
    }

    @ApiOperation(value = "cleanupProjectStorage", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/storage")
    @ResponseBody
    public EnvelopeResponse<Boolean> cleanupProjectStorage(@PathVariable(value = "project") String project)
            throws Exception {
        ProjectInstance projectInstance = projectService.getManager(NProjectManager.class).getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        projectService.cleanupGarbage(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, true, "");
    }

    @ApiOperation(value = "updateStorageQuotaConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/storage_quota")
    @ResponseBody
    public EnvelopeResponse<Boolean> updateStorageQuotaConfig(@PathVariable(value = "project") String project,
            @RequestBody StorageQuotaRequest storageQuotaRequest) {
        checkProjectName(project);
        checkRequiredArg("storage_quota_size", storageQuotaRequest.getStorageQuotaSize());
        projectService.updateStorageQuotaConfig(project, storageQuotaRequest.getStorageQuotaSize());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, true, "");
    }



    @ApiOperation(value = "updateShardNumConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/shard_num_config")
    @ResponseBody
    public EnvelopeResponse<String> updateShardNumConfig(@PathVariable("project") String project,
            @RequestBody ShardNumConfigRequest req) {
        projectService.updateShardNumConfig(project, req);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectService.getShardNumConfig(project), "");
    }

    @ApiOperation(value = "updateGarbageCleanupConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/garbage_cleanup_config")
    @ResponseBody
    public EnvelopeResponse<Boolean> updateGarbageCleanupConfig(@PathVariable("project") String project,
            @RequestBody GarbageCleanUpConfigRequest garbageCleanUpConfigRequest) {
        checkRequiredArg("low_frequency_threshold", garbageCleanUpConfigRequest.getLowFrequencyThreshold());
        checkRequiredArg("frequency_time_window", garbageCleanUpConfigRequest.getFrequencyTimeWindow());
        projectService.updateGarbageCleanupConfig(project, garbageCleanUpConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, true, "");
    }

    @ApiOperation(value = "updateJobNotificationConfig", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/job_notification_config")
    @ResponseBody
    public EnvelopeResponse<String> updateJobNotificationConfig(@PathVariable("project") String project,
            @RequestBody JobNotificationConfigRequest jobNotificationConfigRequest) {
        checkRequiredArg("data_load_empty_notification_enabled",
                jobNotificationConfigRequest.getDataLoadEmptyNotificationEnabled());
        checkRequiredArg("job_error_notification_enabled",
                jobNotificationConfigRequest.getJobErrorNotificationEnabled());
        checkRequiredArg("job_notification_emails", jobNotificationConfigRequest.getJobNotificationEmails());
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updatePushDownConfig", tags = { "QE" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/push_down_config")
    @ResponseBody
    public EnvelopeResponse<String> updatePushDownConfig(@PathVariable("project") String project,
            @RequestBody PushDownConfigRequest pushDownConfigRequest) {
        checkRequiredArg("push_down_enabled", pushDownConfigRequest.getPushDownEnabled());
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateSCD2Config", tags = { "AI" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/scd2_config")
    @ResponseBody
    public EnvelopeResponse<String> updateSCD2Config(@PathVariable("project") String project,
            @RequestBody SCD2ConfigRequest scd2ConfigRequest) {
        checkRequiredArg("scd2_enabled", scd2ConfigRequest.getScd2Enabled());
        projectService.updateSCD2Config(project, scd2ConfigRequest, modelService);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updatePushDownProjectConfig", tags = { "QE" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/push_down_project_config")
    @ResponseBody
    public EnvelopeResponse<String> updatePushDownProjectConfig(@PathVariable("project") String project,
            @RequestBody PushDownProjectConfigRequest pushDownProjectConfigRequest) {
        checkRequiredArg("runner_class_name", pushDownProjectConfigRequest.getRunnerClassName());
        checkRequiredArg("converter_class_names", pushDownProjectConfigRequest.getConverterClassNames());
        projectService.updatePushDownProjectConfig(project, pushDownProjectConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateSnapshotConfig", tags = { "AI" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/snapshot_config")
    @ResponseBody
    public EnvelopeResponse<String> updateSnapshotConfig(@PathVariable("project") String project,
            @RequestBody SnapshotConfigRequest snapshotConfigRequest) {
        checkBooleanArg("snapshot_manual_management_enabled",
                snapshotConfigRequest.getSnapshotManualManagementEnabled());
        projectService.updateSnapshotConfig(project, snapshotConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateExposeComputedColumnConfig", tags = { "QE" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/computed_column_config")
    @ResponseBody
    public EnvelopeResponse<String> updatePushDownConfig(@PathVariable("project") String project,
            @RequestBody ComputedColumnConfigRequest computedColumnConfigRequest) {
        checkRequiredArg("expose_computed_column", computedColumnConfigRequest.getExposeComputedColumn());
        projectService.updateComputedColumnConfig(project, computedColumnConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateSegmentConfig", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/segment_config")
    @ResponseBody
    public EnvelopeResponse<String> updateSegmentConfig(@PathVariable("project") String project,
            @RequestBody SegmentConfigRequest segmentConfigRequest) {
        checkRequiredArg("auto_merge_enabled", segmentConfigRequest.getAutoMergeEnabled());
        checkRequiredArg("auto_merge_time_ranges", segmentConfigRequest.getAutoMergeTimeRanges());
        checkRequiredArg("create_empty_segment_enabled", segmentConfigRequest.getCreateEmptySegmentEnabled());
        projectService.updateSegmentConfig(project, segmentConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectGeneralInfo", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/project_general_info")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectGeneralInfo(@PathVariable("project") String project,
            @RequestBody ProjectGeneralInfoRequest projectGeneralInfoRequest) {
        projectService.updateProjectGeneralInfo(project, projectGeneralInfoRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getProjectConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @GetMapping(value = "/{project:.+}/project_config")
    @ResponseBody
    public EnvelopeResponse<ProjectConfigResponse> getProjectConfig(@PathVariable(value = "project") String project) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectService.getProjectConfig(project), "");
    }

    @ApiOperation(value = "resetProjectConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/project_config")
    @ResponseBody
    public EnvelopeResponse<ProjectConfigResponse> resetProjectConfig(@PathVariable("project") String project,
            @RequestBody ProjectConfigResetRequest projectConfigResetRequest) {
        checkRequiredArg("reset_item", projectConfigResetRequest.getResetItem());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                projectService.resetProjectConfig(project, projectConfigResetRequest.getResetItem()), "");
    }

    @ApiOperation(value = "setDataSourceType", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/source_type")
    @ResponseBody
    public EnvelopeResponse<String> setDataSourceType(@PathVariable("project") String project,
            @RequestBody DataSourceTypeRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        projectService.setDataSourceType(project, request.getSourceType());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateYarnQueue", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/yarn_queue")
    @ResponseBody
    public EnvelopeResponse<String> updateYarnQueue(@PathVariable("project") String project,
            @RequestBody YarnQueueRequest request) {
        checkRequiredArg("queue_name", request.getQueueName());

        projectService.updateYarnQueue(project, request.getQueueName());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectKerberosInfo", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/project_kerberos_info")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectKerberosInfo(@PathVariable("project") String project,
            @RequestParam("file") MultipartFile keytabFile, ProjectKerberosInfoRequest projectKerberosInfoRequest)
            throws Exception {
        File file = projectService.generateTempKeytab(projectKerberosInfoRequest.getPrincipal(), keytabFile);
        projectKerberosInfoRequest.setKeytab(FileUtils.encodeBase64File(file.getAbsolutePath()));
        projectService.updateProjectKerberosInfo(project, projectKerberosInfoRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectOwner", tags = { "SM" })
    @PutMapping(value = "/{project:.+}/owner")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectOwner(@PathVariable("project") String project,
            @RequestBody OwnerChangeRequest request) {
        checkProjectName(project);
        checkRequiredArg("owner", request.getOwner());
        projectService.updateProjectOwner(project, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectConfig", tags = { "SM" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/config")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectConfig(@PathVariable("project") String project,
            @RequestBody Map<String, String> request) {
        checkProjectName(project);
        if (MapUtils.isEmpty(request)) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getConfigMapEmpty());
        }
        if (!Collections.disjoint(request.keySet(), KylinConfig.getInstanceFromEnv().getNonCustomProjectConfigs())) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getConfigNotSupportEdit());
        }
        projectService.updateProjectConfig(project, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteProjectConfig", tags = { "SM" })
    @PostMapping(value = "/config/deletion")
    @ResponseBody
    public EnvelopeResponse<String> deleteProjectConfig(@RequestBody ProjectConfigRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("config_name", request.getConfigName());
        if (KylinConfig.getInstanceFromEnv().getNonCustomProjectConfigs().contains(request.getConfigName())) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getConfigNotSupportDelete());
        }
        projectService.deleteProjectConfig(request.getProject(), request.getConfigName());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "nonCustomConfig", tags = { "SM" })
    @GetMapping(value = "/default_configs")
    @ResponseBody
    public EnvelopeResponse<Set<String>> getNonCustomProjectConfigs() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                KylinConfig.getInstanceFromEnv().getNonCustomProjectConfigs(), "");
    }

    @ApiOperation(value = "update jdbc config (update)", tags = { "QE" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project}/jdbc_config")
    @ResponseBody
    public EnvelopeResponse<Object> updateJdbcConfig(@RequestBody JdbcRequest jdbcRequest,
            @PathVariable(value = "project") String project) {
        checkRequiredArg("project", project);
        projectService.updateJdbcConfig(project, jdbcRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "updateMultiPartitionConfig", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/multi_partition_config")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionConfig(@PathVariable("project") String project,
            @RequestBody MultiPartitionConfigRequest request) {
        checkRequiredArg("multi_partition_enabled", request.getMultiPartitionEnabled());
        projectService.updateMultiPartitionConfig(project, request, modelService);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateJdbcSourceInfoConfig", tags = { "DW" }, notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/jdbc_source_info_config")
    @ResponseBody
    public EnvelopeResponse<String> updateJdbcSourceConfig(@PathVariable("project") String project,
            @RequestBody JdbcSourceInfoRequest request) {
        checkRequiredArg("jdbc_source_enabled", request.getJdbcSourceEnable());
        projectService.updateJdbcInfo(project, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }
}
