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

package io.kyligence.kap.rest.controller2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.ProjectRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 */
@Controller
@RequestMapping(value = "/projects")
public class ProjectControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ProjectControllerV2.class);

    private static final char[] VALID_PROJECTNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getReadableProjectsV2(
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {

        HashMap<String, Object> data = new HashMap<String, Object>();

        List<ProjectInstance> readableProjects = projectService.getReadableProjects(projectName);
        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (readableProjects.size() <= offset) {
            offset = readableProjects.size();
            limit = 0;
        }

        if ((readableProjects.size() - offset) < limit) {
            limit = readableProjects.size() - offset;
        }
        data.put("projects", readableProjects.subList(offset, offset + limit));
        data.put("size", readableProjects.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse saveProjectV2(@RequestBody ProjectRequest projectRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        ProjectInstance projectDesc = deserializeProjectDescV2(projectRequest);

        if (StringUtils.isEmpty(projectDesc.getName())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (!StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECTNAME)) {
            logger.info("Invalid Project name {}, only letters, numbers and underline supported.",
                    projectDesc.getName());
            throw new BadRequestException(String.format(msg.getINVALID_PROJECT_NAME(), projectDesc.getName()));
        }

        ProjectInstance createdProj = null;
        createdProj = projectService.createProject(projectDesc);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createdProj, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateProjectV2(@RequestBody ProjectRequest projectRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        String formerProjectName = projectRequest.getFormerProjectName();
        if (StringUtils.isEmpty(formerProjectName)) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        ProjectInstance projectDesc = deserializeProjectDescV2(projectRequest);

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

    private ProjectInstance deserializeProjectDescV2(ProjectRequest projectRequest) throws IOException {
        logger.debug("Saving project " + projectRequest.getProjectDescData());
        ProjectInstance projectDesc = JsonUtil.readValue(projectRequest.getProjectDescData(), ProjectInstance.class);
        return projectDesc;
    }

    @RequestMapping(value = "/{projectName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteProjectV2(@PathVariable String projectName) throws IOException {
        Message msg = MsgPicker.getMsg();
        ProjectInstance project = projectService.getProjectManager().getProject(projectName);
        if (!isProjectEmpty(projectName)) {
            throw new BadRequestException(msg.getDELETE_PROJECT_NOT_EMPTY());
        }

        projectService.deleteProject(projectName, project);
    }

    private boolean isProjectEmpty(String projectName) throws IOException {
        return cubeService.listAllCubes(projectName).isEmpty()
                && cubeService.listCubeDrafts(null, null, projectName, true).isEmpty()
                && modelService.listAllModels(null, projectName, false).isEmpty()
                && modelService.listModelDrafts(null, projectName).isEmpty();
    }

}
