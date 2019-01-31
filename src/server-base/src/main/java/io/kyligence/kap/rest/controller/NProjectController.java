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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.validation.Valid;

import io.kyligence.kap.rest.request.DataSourceTypeRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.FavoriteQueryThresholdRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.MaintainModelTypeRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.StorageQuotaRequest;
import io.kyligence.kap.rest.service.ProjectService;

@Controller
@RequestMapping(value = "/projects")
public class NProjectController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NProjectController.class);

    private static final Message msg = MsgPicker.getMsg();

    private static final char[] VALID_PROJECT_NAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProjects(@RequestParam(value = "project", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer size) {

        List<ProjectInstance> readableProjects = projectService.getReadableProjects(projectName);
        HashMap<String, Object> projects = getDataResponse("projects", readableProjects, offset, size);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projects, "");

    }

    @RequestMapping(value = "/{project}", method = {RequestMethod.DELETE}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse dropProject(@PathVariable("project") String project) {
        projectService.dropProject(project);
        projectService.clearManagerCache(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

    }

    @RequestMapping(value = "/backup/{project}", method = {RequestMethod.POST}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse backupProject(@PathVariable("project") String project) throws Exception {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.backupProject(project), "");

    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse saveProject(@Valid @RequestBody ProjectRequest projectRequest) {

        ProjectInstance projectDesc = projectService.deserializeProjectDesc(projectRequest);
        if (StringUtils.isEmpty(projectDesc.getName())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }
        if (!StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECT_NAME)) {
            throw new BadRequestException(String.format(msg.getINVALID_PROJECT_NAME(), projectDesc.getName()));
        }
        ProjectInstance createdProj = projectService.createProject(projectDesc.getName(), projectDesc);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createdProj, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updateProject(@Valid @RequestBody ProjectRequest projectRequest) throws IOException {

        String formerProjectName = projectRequest.getFormerProjectName();
        if (StringUtils.isEmpty(formerProjectName)) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        ProjectInstance projectDesc = projectService.deserializeProjectDesc(projectRequest);

        ProjectInstance currentProject = projectService.getProjectManager().getProject(formerProjectName);
        if (currentProject == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), formerProjectName));
        }

        ProjectInstance updatedProj;
        if (projectDesc.getName().equals(currentProject.getName())) {
            updatedProj = projectService.updateProject(formerProjectName, projectDesc, currentProject);
        } else {
            throw new BadRequestException(msg.getPROJECT_RENAME());
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, updatedProj, "");
    }

    @RequestMapping(value = "/query_accelerate_threshold", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updateQueryAccelerateThresholdConfig(
            @RequestBody FavoriteQueryThresholdRequest favoriteQueryThresholdRequest) {
        checkProjectName(favoriteQueryThresholdRequest.getProject());
        checkRequiredArg("threshold", favoriteQueryThresholdRequest.getThreshold());
        projectService.updateQueryAccelerateThresholdConfig(favoriteQueryThresholdRequest.getProject(),
                favoriteQueryThresholdRequest.getThreshold(), favoriteQueryThresholdRequest.isAutoApply(),
                favoriteQueryThresholdRequest.isBatchEnabled());
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

    @RequestMapping(value = "/maintain_model_type", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updateMantainModelType(@RequestBody MaintainModelTypeRequest request) {
        checkProjectName(request.getProject());
        projectService.updateMantainModelType(request.getProject(), request.getMaintainModelType());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

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
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse cleanupProjectStorage(@RequestParam(value = "project", required = true) String project)
            throws Exception {
        ProjectInstance projectInstance = projectService.getProjectManager().getProject(project);
        if (projectInstance == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), project));
        }
        projectService.cleanupGarbage(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

    @RequestMapping(value = "/storage_quota", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updateStorageQuotaConfig(@RequestBody StorageQuotaRequest storageQuotaRequest)
            throws Exception {
        String project = storageQuotaRequest.getProject();
        checkProjectName(project);

        long storageQuotaSize = storageQuotaRequest.getStorageQuotaSize();
        projectService.updateStorageQuotaConfig(project, storageQuotaSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

    @RequestMapping(value = "/job_notification_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updateJobNotificationConfig(
            @RequestBody JobNotificationConfigRequest jobNotificationConfigRequest) {
        checkProjectName(jobNotificationConfigRequest.getProject());
        projectService.updateJobNotificationConfig(jobNotificationConfigRequest.getProject(),
                jobNotificationConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/push_down_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updatePushDownConfig(@RequestBody PushDownConfigRequest pushDownConfigRequest) {
        checkProjectName(pushDownConfigRequest.getProject());
        projectService.updatePushDownConfig(pushDownConfigRequest.getProject(), pushDownConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/segment_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public EnvelopeResponse updateSegmentConfig(@RequestBody SegmentConfigRequest segmentConfigRequest) {
        checkProjectName(segmentConfigRequest.getProject());
        checkSegmentConfigArg(segmentConfigRequest);
        projectService.updateSegmentConfig(segmentConfigRequest.getProject(), segmentConfigRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/project_general_info", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
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

    @RequestMapping(value = "/source_type", method = {RequestMethod.PUT}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse setDataSourceType(@RequestBody DataSourceTypeRequest request) {
        checkProjectName(request.getProject());
        projectService.setDataSourceType(request.getProject(), request.getSourceType());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

}
