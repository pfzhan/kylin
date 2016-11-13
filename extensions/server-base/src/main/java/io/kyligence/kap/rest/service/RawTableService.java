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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableDescManager;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableUpdate;

@Component("rawMgmtService")
public class RawTableService extends BasicService {
    private static final String DESC_SUFFIX = "_desc";

    private static final Logger logger = LoggerFactory.getLogger(RawTableService.class);

    @Autowired
    private AccessService accessService;

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#raw, 'ADMINISTRATION') or hasPermission(#raw, 'MANAGEMENT')")
    public RawTableInstance updateRawCost(RawTableInstance raw, int cost) throws IOException {

        if (raw.getCost() == cost) {
            // Do nothing
            return raw;
        }
        raw.setCost(cost);

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        raw.setOwner(owner);

        RawTableUpdate rawBuilder = new RawTableUpdate(raw).setOwner(owner).setCost(cost);

        return getRawTableManager().updateRawTable(rawBuilder);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or " + Constant.ACCESS_HAS_ROLE_MODELER)
    public RawTableInstance createRawTableInstanceAndDesc(String rawName, String projectName, RawTableDesc desc) throws IOException {
        if (getRawTableManager().getRawTableInstance(rawName) != null) {
            throw new InternalErrorException("The raw named " + rawName + " already exists");
        }

        if (getRawTableDescManager().getRawTableDesc(desc.getName()) != null) {
            throw new InternalErrorException("The raw desc named " + desc.getName() + " already exists");
        }

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        RawTableDesc createdDesc;
        RawTableInstance createdRaw;

        createdDesc = getRawTableDescManager().createRawTableDesc(desc);
        try {
            createdRaw = getRawTableManager().createRawTableInstance(rawName, projectName, createdDesc, owner);
        } catch (Exception e) {
            // if create rawtable instance fails, roll back desc changes
            try {
                getRawTableDescManager().removeRawTableDesc(desc);
            } catch (Exception ex) {
                logger.error("Error when rollback created RawTableDesc", ex);
            }
            throw e;
        }
        accessService.init(createdRaw, AclPermission.ADMINISTRATION);

        ProjectInstance project = getProjectManager().getProject(projectName);
        accessService.inherit(createdRaw, project);

        return createdRaw;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteRaw(RawTableInstance raw) throws IOException, JobException {
        final List<CubingJob> cubingJobs = listAllCubingJobs(raw.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new JobException("The raw " + raw.getName() + " has running job, please discard it and try again.");
        }
        int rawNum = getRawTableManager().getRawTablesByDesc(raw.getDescName()).size();
        getRawTableManager().dropRawTableInstance(raw.getName(), rawNum == 1);//only delete cube desc when no other cube is using it
        accessService.clean(raw, true);
    }

    public RawTableManager getRawTableManager() {
        return RawTableManager.getInstance(getConfig());
    }

    public RawTableDescManager getRawTableDescManager() {
        return RawTableDescManager.getInstance(getConfig());
    }

    private boolean isRawTableInProject(String projectName, RawTableInstance target) {
        ProjectManager projectManager = getProjectManager();
        ProjectInstance project = projectManager.getProject(projectName);
        if (project == null) {
            return false;
        }
        for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
            if (projectDataModel.getType() == RealizationType.INVERTED_INDEX) {
                RawTableInstance raw = getRawTableManager().getRawTableInstance(projectDataModel.getRealization());
                if (raw == null) {
                    logger.error("Project " + projectName + " contains realization " + projectDataModel.getRealization() + " which is not found by RawTableManager");
                    continue;
                }
                if (raw.equals(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#raw, 'ADMINISTRATION') or hasPermission(#raw, 'MANAGEMENT')")
    public RawTableDesc updateRawTableInstanceAndDesc(RawTableInstance raw, RawTableDesc desc, String newProjectName, boolean forceUpdate) throws IOException, JobException {

        final List<CubingJob> cubingJobs = listAllCubingJobs(raw.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new JobException("RawTable schema shouldn't be changed with running job.");
        }

        try {

            RawTableDesc updatedRawTableDesc = getRawTableDescManager().updateRawTableDesc(desc);
            ProjectManager projectManager = getProjectManager();
            if (!isRawTableInProject(newProjectName, raw)) {
                String owner = SecurityContextHolder.getContext().getAuthentication().getName();
                ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.CUBE, raw.getName(), newProjectName, owner);
                accessService.inherit(raw, newProject);
            }

            return updatedRawTableDesc;
        } catch (IOException e) {
            throw new InternalErrorException("Failed to deal with the request.", e);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public RawTableInstance enableRaw(RawTableInstance raw) throws IOException, JobException {
        String cubeName = raw.getName();

        RealizationStatusEnum ostatus = raw.getStatus();
        if (!raw.getStatus().equals(RealizationStatusEnum.DISABLED)) {
            throw new InternalErrorException("Only disabled raw can be enabled, status of " + cubeName + " is " + ostatus);
        }

        if (raw.getSegments(SegmentStatusEnum.READY).size() == 0) {
            throw new InternalErrorException("Raw " + cubeName + " dosen't contain any READY segment");
        }

        final List<CubingJob> cubingJobs = listAllCubingJobs(raw.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new JobException("Enable is not allowed with a running job.");
        }

        try {
            RawTableUpdate builder = new RawTableUpdate(raw);
            builder.setStatus(RealizationStatusEnum.READY);
            return getRawTableManager().updateRawTable(builder);
        } catch (IOException e) {
            raw.setStatus(ostatus);
            throw e;
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public RawTableInstance disableRaw(RawTableInstance raw) throws IOException, JobException {

        String cubeName = raw.getName();

        RealizationStatusEnum ostatus = raw.getStatus();
        if (null != ostatus && !RealizationStatusEnum.READY.equals(ostatus)) {
            throw new InternalErrorException("Only ready raw can be disabled, status of " + cubeName + " is " + ostatus);
        }

        raw.setStatus(RealizationStatusEnum.DISABLED);

        try {
            RawTableUpdate builder = new RawTableUpdate(raw);
            builder.setStatus(RealizationStatusEnum.DISABLED);
            return getRawTableManager().updateRawTable(builder);
        } catch (IOException e) {
            raw.setStatus(ostatus);
            throw e;
        }
    }

    public static String getRawTableNameFromDesc(String descName) {
        if (descName.toLowerCase().endsWith(DESC_SUFFIX)) {
            return descName.substring(0, descName.toLowerCase().indexOf(DESC_SUFFIX));
        } else {
            return descName;
        }
    }
}
