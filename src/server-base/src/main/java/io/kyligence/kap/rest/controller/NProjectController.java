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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;
import java.util.Objects;

import javax.validation.Valid;

import io.kyligence.kap.rest.request.ComputedColumnConfigRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.AclPermissionFactory;
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

import io.kyligence.kap.rest.request.DataSourceTypeRequest;
import io.kyligence.kap.rest.request.DefaultDatabaseRequest;
import io.kyligence.kap.rest.request.FavoriteQueryThresholdRequest;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.ProjectConfigResetRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.request.StorageQuotaRequest;
import io.kyligence.kap.rest.request.YarnQueueRequest;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import io.kyligence.kap.rest.response.ProjectConfigResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
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
    @Qualifier("projectService")
    private ProjectService projectService;

    @ApiOperation(value = "getProjects (update)", notes = "Update Param: page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ProjectInstance>>> getProjects(
            @RequestParam(value = "project", required = false) String projectName,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer size,
            @RequestParam(value = "exact", required = false, defaultValue = "false") boolean exactMatch,
            @RequestParam(value = "permission", required = false, defaultValue = "READ") String permission) {
        if (Objects.isNull(AclPermissionFactory.getPermission(permission))) {
            throw new BadRequestException("Operation failed, unknown permission:" + permission);
        }
        List<ProjectInstance> projects = projectService.getProjectsFilterByExactMatchAndPermission(projectName, exactMatch, permission);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(projects, offset, size), "");
    }

    @DeleteMapping(value = "/{project:.+}")
    @ResponseBody
    public EnvelopeResponse<String> dropProject(@PathVariable("project") String project) {
        projectService.dropProject(project);
        projectService.clearManagerCache(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "backupProject (check)", notes = "Update URL, {project}")
    @PostMapping(value = "/{project:.+}/backup")
    @ResponseBody
    public EnvelopeResponse<String> backupProject(@PathVariable("project") String project) throws Exception {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, projectService.backupProject(project), "");
    }

    @ApiOperation(value = "saveProject (update)", notes = "Update Param: former_project_name, project_desc_data")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<ProjectInstance> saveProject(@Valid @RequestBody ProjectRequest projectRequest) {
        checkRequiredArg("maintain_model_type", projectRequest.getMaintainModelType());

        ProjectInstance projectDesc = new ProjectInstance();
        BeanUtils.copyProperties(projectRequest, projectDesc);
        checkRequiredArg("name", projectRequest.getName());
        if (StringUtils.isEmpty(projectRequest.getName())
                || !StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECT_NAME)) {
            throw new BadRequestException(MsgPicker.getMsg().getINVALID_PROJECT_NAME(), projectDesc.getName());
        }

        ProjectInstance createdProj = projectService.createProject(projectDesc.getName(), projectDesc);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, createdProj, "");
    }

    @ApiOperation(value = "updateDefaultDatabase (update)", notes = "Add URL: {project}; Update Param: default_database;")
    @PutMapping(value = "/{project:.+}/default_database")
    @ResponseBody
    public EnvelopeResponse updateDefaultDatabase(@PathVariable("project") String project,
            @RequestBody DefaultDatabaseRequest defaultDatabaseRequest) {
        checkRequiredArg("default_database", defaultDatabaseRequest.getDefaultDatabase());

        projectService.updateDefaultDatabase(project, defaultDatabaseRequest.getDefaultDatabase());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateQueryAccelerateThresholdConfig (update)", notes = "Add URL: {project}; ")
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
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getQueryAccelerateThresholdConfig (update)", notes = "Add URL: {project}; ")
    @GetMapping(value = "/{project:.+}/query_accelerate_threshold")
    @ResponseBody
    public EnvelopeResponse<FavoriteQueryThresholdResponse> getQueryAccelerateThresholdConfig(
            @PathVariable(value = "project") String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                projectService.getQueryAccelerateThresholdConfig(project), "");
    }

    @ApiOperation(value = "getStorageVolumeInfo (update)", notes = "Add URL: {project}; ")
    @GetMapping(value = "/{project:.+}/storage_volume_info")
    @ResponseBody
    public EnvelopeResponse<StorageVolumeInfoResponse> getStorageVolumeInfo(
            @PathVariable(value = "project") String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, projectService.getStorageVolumeInfoResponse(project),
                "");
    }

    @ApiOperation(value = "cleanupProjectStorage (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/storage")
    @ResponseBody
    public EnvelopeResponse<Boolean> cleanupProjectStorage(@PathVariable(value = "project") String project)
            throws Exception {
        ProjectInstance projectInstance = projectService.getProjectManager().getProject(project);
        if (projectInstance == null) {
            throw new BadRequestException(String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        projectService.cleanupGarbage(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
    }

    @ApiOperation(value = "updateStorageQuotaConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/storage_quota")
    @ResponseBody
    public EnvelopeResponse<Boolean> updateStorageQuotaConfig(@PathVariable(value = "project") String project,
            @RequestBody StorageQuotaRequest storageQuotaRequest) {
        checkProjectName(project);
        checkRequiredArg("storage_quota_size", storageQuotaRequest.getStorageQuotaSize());
        projectService.updateStorageQuotaConfig(project, storageQuotaRequest.getStorageQuotaSize());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
    }

    @ApiOperation(value = "updateShardNumConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/shard_num_config")
    @ResponseBody
    public EnvelopeResponse<String> updateShardNumConfig(@PathVariable("project") String project,
            @RequestBody ShardNumConfigRequest req) {
        projectService.updateShardNumConfig(project, req);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, projectService.getShardNumConfig(project), "");
    }

    @ApiOperation(value = "updateGarbageCleanupConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/garbage_cleanup_config")
    @ResponseBody
    public EnvelopeResponse updateGarbageCleanupConfig(@PathVariable("project") String project,
            @RequestBody GarbageCleanUpConfigRequest garbageCleanUpConfigRequest) {
        checkRequiredArg("low_frequency_threshold", garbageCleanUpConfigRequest.getLowFrequencyThreshold());
        checkRequiredArg("frequency_time_window", garbageCleanUpConfigRequest.getFrequencyTimeWindow());
        projectService.updateGarbageCleanupConfig(project, garbageCleanUpConfigRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
    }

    @ApiOperation(value = "updateJobNotificationConfig (update)", notes = "Add URL: {project}; ")
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
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updatePushDownConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/push_down_config")
    @ResponseBody
    public EnvelopeResponse<String> updatePushDownConfig(@PathVariable("project") String project,
            @RequestBody PushDownConfigRequest pushDownConfigRequest) {
        checkRequiredArg("push_down_enabled", pushDownConfigRequest.getPushDownEnabled());
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateExposeComputedColumnConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/computed_column_config")
    @ResponseBody
    public EnvelopeResponse<String> updatePushDownConfig(@PathVariable("project") String project,
                                                         @RequestBody ComputedColumnConfigRequest computedColumnConfigRequest) {
        checkRequiredArg("expose_computed_column", computedColumnConfigRequest.getExposeComputedColumn());
        projectService.updateComputedColumnConfig(project, computedColumnConfigRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateSegmentConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/segment_config")
    @ResponseBody
    public EnvelopeResponse<String> updateSegmentConfig(@PathVariable("project") String project,
            @RequestBody SegmentConfigRequest segmentConfigRequest) {
        checkRequiredArg("auto_merge_enabled", segmentConfigRequest.getAutoMergeEnabled());
        checkRequiredArg("auto_merge_time_ranges", segmentConfigRequest.getAutoMergeTimeRanges());
        projectService.updateSegmentConfig(project, segmentConfigRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectGeneralInfo (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/project_general_info")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectGeneralInfo(@PathVariable("project") String project,
            @RequestBody ProjectGeneralInfoRequest projectGeneralInfoRequest) {
        projectService.updateProjectGeneralInfo(project, projectGeneralInfoRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getProjectConfig (update)", notes = "Add URL: {project}; ")
    @GetMapping(value = "/{project:.+}/project_config")
    @ResponseBody
    public EnvelopeResponse<ProjectConfigResponse> getProjectConfig(@PathVariable(value = "project") String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, projectService.getProjectConfig(project), "");
    }

    @ApiOperation(value = "resetProjectConfig (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/project_config")
    @ResponseBody
    public EnvelopeResponse<ProjectConfigResponse> resetProjectConfig(@PathVariable("project") String project,
            @RequestBody ProjectConfigResetRequest projectConfigResetRequest) {
        checkRequiredArg("reset_item", projectConfigResetRequest.getResetItem());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                projectService.resetProjectConfig(project, projectConfigResetRequest.getResetItem()), "");
    }

    @ApiOperation(value = "setDataSourceType (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/source_type")
    @ResponseBody
    public EnvelopeResponse<String> setDataSourceType(@PathVariable("project") String project,
            @RequestBody DataSourceTypeRequest request) {
        projectService.setDataSourceType(project, request.getSourceType());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateYarnQueue (update)", notes = "Add URL: {project}; ")
    @PutMapping(value = "/{project:.+}/yarn_queue")
    @ResponseBody
    public EnvelopeResponse<String> updateYarnQueue(@PathVariable("project") String project,
            @RequestBody YarnQueueRequest request) {
        checkRequiredArg("queue_name", request.getQueueName());

        projectService.updateYarnQueue(project, request.getQueueName());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}
