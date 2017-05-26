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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.util.AclUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * @author xduo
 * 
 */
@Component("projectService")
public class ProjectService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    private AclUtil aclUtil;

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public ProjectInstance createProject(ProjectInstance newProject) throws IOException {
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
        accessService.init(createdProject, AclPermission.ADMINISTRATION);
        logger.debug("New project created.");

        return createdProject;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#currentProject, 'ADMINISTRATION') or hasPermission(#currentProject, 'MANAGEMENT')")
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

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<ProjectInstance> listProjects(final Integer limit, final Integer offset) {
        List<ProjectInstance> projects = listAllProjects(limit, offset);
        return projects;
    }

    @Deprecated
    public List<ProjectInstance> listAllProjects(final Integer limit, final Integer offset) {
        List<ProjectInstance> projects = getProjectManager().listAllProjects();

        int climit = (null == limit) ? Integer.MAX_VALUE : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (projects.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((projects.size() - coffset) < climit) {
            return projects.subList(coffset, projects.size());
        }

        return projects.subList(coffset, coffset + climit);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public void deleteProject(String projectName, ProjectInstance project) throws IOException {
        getProjectManager().dropProject(projectName);

        accessService.clean(project, true);
    }

    public boolean isTableInAnyProject(String tableName) {
        for (ProjectInstance projectInstance : ProjectManager.getInstance(getConfig()).listAllProjects()) {
            if (projectInstance.containsTable(tableName.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    public boolean isTableInProject(String tableName, String projectName) {
        ProjectInstance projectInstance = ProjectManager.getInstance(getConfig()).getProject(projectName);
        if (projectInstance != null) {
            if (projectInstance.containsTable(tableName.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    public String getProjectOfModel(String modelName) {
        List<ProjectInstance> projectInstances = listProjects(null, null);
        for (ProjectInstance projectInstance : projectInstances) {
            if (projectInstance.containsModel(modelName))
                return projectInstance.getName();
        }
        return null;
    }

    public List<ProjectInstance> getReadableProjects() {
        List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();

        //list all projects first
        List<ProjectInstance> projectInstances = getProjectManager().listAllProjects();

        for (ProjectInstance projectInstance : projectInstances) {

            if (projectInstance == null) {
                continue;
            }

            boolean hasProjectPermission = false;
            try {
                hasProjectPermission = aclUtil.hasProjectReadPermission(projectInstance);
            } catch (AccessDeniedException e) {
                //ignore to continue
            }

            if (!hasProjectPermission) {
                List<CubeInstance> cubeInstances = cubeService.listAllCubes(projectInstance.getName());

                for (CubeInstance cubeInstance : cubeInstances) {
                    if (cubeInstance == null) {
                        continue;
                    }

                    try {
                        aclUtil.hasCubeReadPermission(cubeInstance);
                        hasProjectPermission = true;
                        break;
                    } catch (AccessDeniedException e) {
                        //ignore to continue
                    }
                }
            }
            if (hasProjectPermission) {
                readableProjects.add(projectInstance);
            }

        }
        return readableProjects;
    }
}
