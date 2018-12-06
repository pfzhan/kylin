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

import io.kyligence.kap.rest.request.MaintainModelTypeRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.FavoriteQueryThresholdRequest;
import io.kyligence.kap.rest.request.StorageQuotaRequest;
import io.kyligence.kap.rest.service.GarbageCleanService;
import io.kyligence.kap.rest.service.ProjectService;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@Controller
@RequestMapping(value = "/projects")
public class NProjectController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NProjectController.class);

    private static final Message msg = MsgPicker.getMsg();

    private static final char[] VALID_PROJECTNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("garbageCleanService")
    GarbageCleanService garbageCleanService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProjects(@RequestParam(value = "project", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer size) {

        List<ProjectInstance> readableProjects = projectService.getReadableProjects(projectName);
        HashMap<String, Object> projects = getDataResponse("projects", readableProjects, offset, size);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projects, "");

    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse saveProject(@RequestBody ProjectRequest projectRequest) throws IOException, PersistentException {

        ProjectInstance projectDesc = projectService.deserializeProjectDesc(projectRequest);
        if (StringUtils.isEmpty(projectDesc.getName())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }
        if (!StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECTNAME)) {
            logger.info("Invalid Project name {}, only letters, numbers and underline supported.",
                    projectDesc.getName());
            throw new BadRequestException(String.format(msg.getINVALID_PROJECT_NAME(), projectDesc.getName()));
        }
        ProjectInstance createdProj = projectService.createProject(projectDesc);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createdProj, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateProject(@RequestBody ProjectRequest projectRequest) throws IOException {

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
            updatedProj = projectService.updateProject(projectDesc, currentProject);
        } else {
            throw new BadRequestException(msg.getPROJECT_RENAME());
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, updatedProj, "");
    }

    @RequestMapping(value = "/query_accelerate_threshold", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateQueryAccelerateThresholdConfig(@RequestBody FavoriteQueryThresholdRequest favoriteQueryThresholdRequest) throws IOException {
        checkProjectName(favoriteQueryThresholdRequest.getProject());
        checkRequiredArg("threshold", favoriteQueryThresholdRequest.getThreshold());
        projectService.updateQueryAccelerateThresholdConfig(favoriteQueryThresholdRequest.getProject(), favoriteQueryThresholdRequest.getThreshold(),
                favoriteQueryThresholdRequest.isAutoApply(), favoriteQueryThresholdRequest.isBatchEnabled());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

    }

    @RequestMapping(value = "/query_accelerate_threshold", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getQueryAccelerateThresholdConfig(
            @RequestParam(value = "project", required = true) String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.getQueryAccelerateThresholdConfig(project), "");
    }

    @RequestMapping(value = "/maintain_model_type", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateMantainModelType(@RequestBody MaintainModelTypeRequest request) throws IOException {
        checkProjectName(request.getProject());
        projectService.updateMantainModelType(request.getProject(), request.getMaintainModelType());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");

    }

    @RequestMapping(value = "/storage_volume_info", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getStorageVolumeInfo(
            @RequestParam(value = "project", required = true) String project) throws Exception {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectService.getStorageVolumeInfoResponse(project), "");
    }

    @RequestMapping(value = "/storage", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cleanupProjectStorage(
            @RequestParam(value = "project", required = true) String project) throws Exception {
        ProjectInstance projectInstance = projectService.getProjectManager().getProject(project);
        if (projectInstance == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), project));
        }
        projectService.cleanupProjectGarbageIndex(project);
        garbageCleanService.cleanupProject(projectInstance);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

    @RequestMapping(value = "/storage_quota", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateStorageQuotaConfig(
            @RequestBody StorageQuotaRequest storageQuotaRequest) throws Exception {
        String project = storageQuotaRequest.getProject();
        checkProjectName(project);

        long storageQuotaSize = storageQuotaRequest.getStorageQuotaSize();
        projectService.updateStorageQuotaConfig(project, storageQuotaSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, true, "");
    }

}
