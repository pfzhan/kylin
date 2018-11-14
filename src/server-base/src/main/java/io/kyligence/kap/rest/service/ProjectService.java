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

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.event.model.AddProjectEvent;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import lombok.val;
import org.apache.directory.api.util.Strings;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.List;

@Component("projectService")
public class ProjectService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    public ProjectInstance deserializeProjectDesc(ProjectRequest projectRequest) throws IOException {
        logger.debug("Saving project " + projectRequest.getProjectDescData());
        ProjectInstance projectDesc = JsonUtil.readValue(projectRequest.getProjectDescData(), ProjectInstance.class);
        return projectDesc;
    }

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
                overrideProps, newProject.getMaintainModelType());
        AddProjectEvent projectEvent = new AddProjectEvent(createdProject.getName());
        getEventManager(createdProject.getName()).post(projectEvent);
        createDefaultRules(projectName);
        logger.debug("New project created.");
        return createdProject;
    }

    private void createDefaultRules(String projectName) throws IOException {
        // create default rules
        // frequency rule
        FavoriteRule.Condition freqCond = new FavoriteRule.Condition();
        freqCond.setRightThreshold("0.1");
        FavoriteRule freqRule = new FavoriteRule(Lists.newArrayList(freqCond), FavoriteRule.FREQUENCY_RULE_NAME, true);
        getFavoriteRuleManager(projectName).createRule(freqRule);
        // submitter rule
        FavoriteRule.Condition submitterCond = new FavoriteRule.Condition();
        submitterCond.setRightThreshold("ADMIN");
        FavoriteRule submitterRule = new FavoriteRule(Lists.newArrayList(submitterCond), FavoriteRule.SUBMITTER_RULE_NAME, true);
        getFavoriteRuleManager(projectName).createRule(submitterRule);
        // duration rule
        FavoriteRule.Condition durationCond = new FavoriteRule.Condition();
        durationCond.setLeftThreshold("0");
        durationCond.setRightThreshold("180");
        FavoriteRule durationRule = new FavoriteRule(Lists.newArrayList(durationCond), FavoriteRule.DURATION_RULE_NAME, false);
        getFavoriteRuleManager(projectName).createRule(durationRule);

        // create blacklist and whitelist
        FavoriteRule blacklist = new FavoriteRule();
        blacklist.setName(FavoriteRule.BLACKLIST_NAME);
        getFavoriteRuleManager(projectName).createRule(blacklist);

        FavoriteRule whitelist = new FavoriteRule();
        whitelist.setName(FavoriteRule.WHITELIST_NAME);
        getFavoriteRuleManager(projectName).createRule(whitelist);
    }

    public List<ProjectInstance> getReadableProjects(final String projectName) {
        List<ProjectInstance> projectInstances = new ArrayList<ProjectInstance>();
        if (!Strings.isEmpty(projectName)) {
            ProjectInstance projectInstance = getProjectManager().getProject(projectName);
            projectInstances.add(projectInstance);
        } else {
            projectInstances.addAll(getProjectManager().listAllProjects());
        }
        return projectInstances;
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

    public void updateQueryAccelerateThresholdConfig(String project, Integer threshold, boolean autoApply,
            boolean batchEnabled) throws IOException {
        NProjectManager projectManager = getProjectManager();
        ProjectInstance projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new BadRequestException("Project '" + project + "' does not exist!");
        }
        ProjectInstance updateProject = projectManager.copyForWrite(projectInstance);
        updateProject.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold", threshold.toString());
        updateProject.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-batch-enable",
                batchEnabled + "");
        updateProject.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-auto-apply",
                autoApply + "");
        projectManager.updateProject(updateProject);
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

    public void updateMantainModelType(String project, String maintainModelType) throws IOException {
        val projectManager = getProjectManager();
        val projectUpdate = projectManager.copyForWrite(projectManager.getProject(project));
        projectUpdate.setMaintainModelType(MaintainModelType.valueOf(maintainModelType));
        projectManager.updateProject(projectUpdate);
    }
}
