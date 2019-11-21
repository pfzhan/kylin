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

import java.util.HashMap;
import java.util.List;

import javax.validation.Valid;

import io.kyligence.kap.rest.request.YarnQueueRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
import io.kyligence.kap.rest.service.ProjectService;

@Controller
@RequestMapping(value = "/api/projects")
public class NProjectController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NProjectController.class);

    private static final char[] VALID_PROJECT_NAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProjects(@RequestParam(value = "project", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer size,
            @RequestParam(value = "exact", required = false, defaultValue = "false") boolean exactMatch) {

        List<ProjectInstance> readableProjects = projectService.getReadableProjects(projectName, exactMatch);
        HashMap<String, Object> projects = getDataResponse("projects", readableProjects, offset, size);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projects, "");

    }

    @RequestMapping(value = "/{project}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse dropProject(@PathVariable("project") String project) {
        projectService.dropProject(project);
        projectService.clearManagerCache(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

    }

    @RequestMapping(value = "/backup/{project}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse backupProject(@PathVariable("project") String project) throws Exception {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.backupProject(project), "");

    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse saveProject(@Valid @RequestBody ProjectRequest projectRequest) {
        checkRequiredArg("maintain_model_type", projectRequest.getMaintainModelType());

        ProjectInstance projectDesc = new ProjectInstance();
        BeanUtils.copyProperties(projectRequest, projectDesc);
        checkRequiredArg("name", projectRequest.getName());
        if (StringUtils.isEmpty(projectRequest.getName())
                || !StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECT_NAME)) {
            throw new BadRequestException(MsgPicker.getMsg().getINVALID_PROJECT_NAME(), projectDesc.getName());
        }
        ProjectInstance createdProj = projectService.createProject(projectDesc.getName(), projectDesc);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createdProj, "");
    }

    @RequestMapping(value = "/default_database", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateDefaultDatabase(@RequestBody DefaultDatabaseRequest defaultDatabaseRequest) {
        checkProjectName(defaultDatabaseRequest.getProject());
        checkRequiredArg("default_database", defaultDatabaseRequest.getDefaultDatabase());

        projectService.updateDefaultDatabase(defaultDatabaseRequest.getProject(),
                defaultDatabaseRequest.getDefaultDatabase());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/query_accelerate_threshold", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateQueryAccelerateThresholdConfig(
            @RequestBody FavoriteQueryThresholdRequest favoriteQueryThresholdRequest) {
        checkProjectName(favoriteQueryThresholdRequest.getProject());
        checkRequiredArg("tips_enabled", favoriteQueryThresholdRequest.getTipsEnabled());
        if (Boolean.TRUE.equals(favoriteQueryThresholdRequest.getTipsEnabled())) {
            checkRequiredArg("threshold", favoriteQueryThresholdRequest.getThreshold());
        }
        projectService.updateQueryAccelerateThresholdConfig(favoriteQueryThresholdRequest.getProject(),
                favoriteQueryThresholdRequest.getThreshold(), favoriteQueryThresholdRequest.getTipsEnabled());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

    }

    @RequestMapping(value = "/query_accelerate_threshold", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getQueryAccelerateThresholdConfig(
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                projectService.getQueryAccelerateThresholdConfig(project), "");
    }

    @RequestMapping(value = "/storage_volume_info", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getStorageVolumeInfo(@RequestParam(value = "project", required = true) String project)
            throws Exception {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.getStorageVolumeInfoResponse(project),
                "");
    }

    @RequestMapping(value = "/storage", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cleanupProjectStorage(@RequestParam(value = "project", required = true) String project)
            throws Exception {
        ProjectInstance projectInstance = projectService.getProjectManager().getProject(project);
        if (projectInstance == null) {
            throw new BadRequestException(String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), project));
        }
        projectService.cleanupGarbage(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

    @RequestMapping(value = "/storage_quota", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateStorageQuotaConfig(@RequestBody StorageQuotaRequest storageQuotaRequest)
            throws Exception {
        String project = storageQuotaRequest.getProject();
        checkProjectName(project);
        checkRequiredArg("storage_quota_size", storageQuotaRequest.getStorageQuotaSize());
        projectService.updateStorageQuotaConfig(project, storageQuotaRequest.getStorageQuotaSize());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

    @RequestMapping(value = "/shard_num_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateShardNumConfig(@RequestBody ShardNumConfigRequest req) {
        String project = req.getProject();
        checkProjectName(project);
        projectService.updateShardNumConfig(project, req);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.getShardNumConfig(project), "");
    }

    @RequestMapping(value = "/garbage_cleanup_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateGarbageCleanupConfig(
            @RequestBody GarbageCleanUpConfigRequest garbageCleanUpConfigRequest) throws Exception {
        String project = garbageCleanUpConfigRequest.getProject();
        checkProjectName(project);
        checkRequiredArg("low_frequency_threshold", garbageCleanUpConfigRequest.getLowFrequencyThreshold());
        checkRequiredArg("frequency_time_window", garbageCleanUpConfigRequest.getFrequencyTimeWindow());
        projectService.updateGarbageCleanupConfig(project, garbageCleanUpConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

    @RequestMapping(value = "/job_notification_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateJobNotificationConfig(
            @RequestBody JobNotificationConfigRequest jobNotificationConfigRequest) {
        checkProjectName(jobNotificationConfigRequest.getProject());
        checkRequiredArg("data_load_empty_notification_enabled",
                jobNotificationConfigRequest.getDataLoadEmptyNotificationEnabled());
        checkRequiredArg("job_error_notification_enabled",
                jobNotificationConfigRequest.getJobErrorNotificationEnabled());
        checkRequiredArg("job_notification_emails", jobNotificationConfigRequest.getJobNotificationEmails());
        projectService.updateJobNotificationConfig(jobNotificationConfigRequest.getProject(),
                jobNotificationConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/push_down_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updatePushDownConfig(@RequestBody PushDownConfigRequest pushDownConfigRequest) {
        checkProjectName(pushDownConfigRequest.getProject());
        checkRequiredArg("push_down_enabled", pushDownConfigRequest.getPushDownEnabled());
        projectService.updatePushDownConfig(pushDownConfigRequest.getProject(), pushDownConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/segment_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateSegmentConfig(@RequestBody SegmentConfigRequest segmentConfigRequest) {
        checkProjectName(segmentConfigRequest.getProject());
        checkRequiredArg("auto_merge_enabled", segmentConfigRequest.getAutoMergeEnabled());
        checkRequiredArg("auto_merge_time_ranges", segmentConfigRequest.getAutoMergeTimeRanges());
        projectService.updateSegmentConfig(segmentConfigRequest.getProject(), segmentConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/project_general_info", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateProjectGeneralInfo(@RequestBody ProjectGeneralInfoRequest projectGeneralInfoRequest) {
        checkProjectName(projectGeneralInfoRequest.getProject());
        projectService.updateProjectGeneralInfo(projectGeneralInfoRequest.getProject(), projectGeneralInfoRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/project_config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProjectConfig(@RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.getProjectConfig(project), "");
    }

    @RequestMapping(value = "/{project}/project_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse resetProjectConfig(@RequestBody ProjectConfigResetRequest projectConfigResetRequest,
            @PathVariable(value = "project") String project) {
        checkRequiredArg("reset_item", projectConfigResetRequest.getResetItem());
        checkRequiredArg("project", project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                projectService.resetProjectConfig(project, projectConfigResetRequest.getResetItem()), "");

    }

    @RequestMapping(value = "/source_type", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setDataSourceType(@RequestBody DataSourceTypeRequest request) {
        checkProjectName(request.getProject());
        projectService.setDataSourceType(request.getProject(), request.getSourceType());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/yarn_queue", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateYarnQueue(@RequestBody YarnQueueRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("queue_name", request.getQueueName());

        projectService.updateYarnQueue(request.getProject(), request.getQueueName());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }
}
