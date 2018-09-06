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

import io.kyligence.kap.rest.request.ProjectRequest;
import io.kylingence.kap.event.model.AddProjectEvent;
import org.apache.directory.api.util.Strings;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.List;

@Component("projectService")
public class ProjectService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private AccessService accessService;

    public ProjectInstance deserializeProjectDesc(ProjectRequest projectRequest) throws IOException {
        logger.debug("Saving project " + projectRequest.getProjectDescData());
        ProjectInstance projectDesc = JsonUtil.readValue(projectRequest.getProjectDescData(), ProjectInstance.class);
        return projectDesc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public ProjectInstance createProject(ProjectInstance newProject) throws IOException, PersistentException {
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
                overrideProps);
        AddProjectEvent projectEvent = new AddProjectEvent(createdProject.getName());
        getEventManager(createdProject.getName()).post(projectEvent);
        logger.debug("New project created.");
        return createdProject;
    }

    public List<ProjectInstance> getReadableProjects(final String projectName) {
        List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();
        List<ProjectInstance> projectInstances = new ArrayList<ProjectInstance>();
        if (!Strings.isEmpty(projectName)) {
            ProjectInstance projectInstance = getProjectManager().getProject(projectName);
            projectInstances.add(projectInstance);
        } else {
            projectInstances = getProjectManager().listAllProjects();
        }
        //list all projects first

        for (ProjectInstance projectInstance : projectInstances) {

            if (projectInstance == null) {
                continue;
            }
            boolean hasProjectPermission = aclEvaluate.hasProjectReadPermission(projectInstance);
            if (hasProjectPermission) {
                readableProjects.add(projectInstance);
            }

        }

        return readableProjects;
    }

    public ProjectInstance updateProject(ProjectInstance newProject, ProjectInstance currentProject)
            throws IOException {

        String newProjectName = newProject.getName();
        String newDescription = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();

        ProjectInstance updatedProject = getProjectManager().updateProject(currentProject, newProjectName,
                newDescription, overrideProps);

        logger.debug("Project updated.");
        return updatedProject;
    }
}
