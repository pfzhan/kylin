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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableDescManager;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableUpdate;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

@Component("rawTableService")
public class RawTableService extends BasicService {
    protected static final String DESC_SUFFIX = "_desc";

    private static final Logger logger = LoggerFactory.getLogger(RawTableService.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#raw, 'ADMINISTRATION') or hasPermission(#raw, 'MANAGEMENT')")
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
    public RawTableInstance createRawTableInstanceAndDesc(String rawName, String projectName, RawTableDesc desc)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (getRawTableManager().getRawTableInstance(rawName) != null) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_ALREADY_EXIST(), rawName));
        }

        if (getRawTableDescManager().getRawTableDesc(desc.getName()) != null) {
            throw new BadRequestException(String.format(msg.getRAW_DESC_ALREADY_EXIST(), desc.getName()));
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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteRaw(RawTableInstance raw) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(raw.getName(), null,
                EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_HAS_RUNNING_JOB(), raw.getName()));
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

    protected boolean isRawTableInProject(String projectName, RawTableInstance target) {
        ProjectManager projectManager = getProjectManager();
        ProjectInstance project = projectManager.getProject(projectName);
        if (project == null) {
            return false;
        }
        for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
            if (projectDataModel.getType() == RealizationType.INVERTED_INDEX) {
                RawTableInstance raw = getRawTableManager().getRawTableInstance(projectDataModel.getRealization());
                if (raw == null) {
                    logger.error("Project " + projectName + " contains realization " + projectDataModel.getRealization()
                            + " which is not found by RawTableManager");
                    continue;
                }
                if (raw.equals(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#raw, 'ADMINISTRATION') or hasPermission(#raw, 'MANAGEMENT')")
    public RawTableDesc updateRawTableInstanceAndDesc(RawTableInstance raw, RawTableDesc desc, String newProjectName,
            boolean forceUpdate) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(raw.getName(), null,
                EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(msg.getRAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB());
        }

        RawTableDesc updatedRawTableDesc = getRawTableDescManager().updateRawTableDesc(desc);
        ProjectManager projectManager = getProjectManager();
        if (!isRawTableInProject(newProjectName, raw)) {
            String owner = SecurityContextHolder.getContext().getAuthentication().getName();
            ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.CUBE, raw.getName(),
                    newProjectName, owner);
            accessService.inherit(raw, newProject);
        }

        return updatedRawTableDesc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public RawTableInstance enableRaw(RawTableInstance raw) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String cubeName = raw.getName();

        RealizationStatusEnum ostatus = raw.getStatus();
        if (!raw.getStatus().equals(RealizationStatusEnum.DISABLED)) {
            throw new BadRequestException(String.format(msg.getENABLE_NOT_DISABLED_RAWTABLE(), cubeName, ostatus));
        }

        if (raw.getSegments(SegmentStatusEnum.READY).size() == 0) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_NO_READY_SEGMENT(), cubeName));
        }

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(raw.getName(), null,
                EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(msg.getRAWTABLE_ENABLE_WITH_RUNNING_JOB());
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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public RawTableInstance disableRaw(RawTableInstance raw) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String cubeName = raw.getName();

        RealizationStatusEnum ostatus = raw.getStatus();
        if (null != ostatus && !RealizationStatusEnum.READY.equals(ostatus)) {
            throw new BadRequestException(String.format(msg.getDISABLE_NOT_READY_RAWTABLE(), cubeName, ostatus));
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

    public void validateRawTableDesc(RawTableDesc desc) {
        KapMessage msg = KapMsgPicker.getMsg();

        if (desc == null) {
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        }

        String descName = desc.getName();
        String name = getRawTableNameFromDesc(descName);

        if (StringUtils.isEmpty(name)) {
            logger.info("RawTable name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_RAWTABLE_NAME());
        }
    }

    public boolean unifyRawTableDesc(RawTableDesc desc, boolean isDraft) throws IOException {
        boolean createNew = false;
        String name = desc.getName();
        if (isDraft) {
            name += "_draft";
            desc.setName(name);
            desc.setDraft(true);
        } else {
            desc.setDraft(false);
        }

        RawTableDesc youngerSelf = killSameUuid(desc.getUuid(), name, isDraft);
        if (youngerSelf != null) {
            desc.setLastModified(youngerSelf.getLastModified());
        } else {
            createNew = true;
            desc.setLastModified(0);
        }

        return createNew;
    }

    public RawTableDesc killSameUuid(String uuid, String name, boolean isDraft) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableDesc youngerSelf = null, official = null;
        boolean rename = false;
        List<RawTableInstance> rawTables = getRawTableManager().listAllRawTables();
        for (RawTableInstance rawTable : rawTables) {
            RawTableDesc rawTableDesc = rawTable.getRawTableDesc();
            if (rawTableDesc.getUuid().equals(uuid)) {
                boolean toDrop = true;
                boolean sameStatus = rawTableDesc.isDraft() == isDraft;
                if (sameStatus && !rawTableDesc.getName().equals(name)) {
                    rename = true;
                }
                if (sameStatus && rawTableDesc.getName().equals(name)) {
                    youngerSelf = rawTableDesc;
                    toDrop = false;
                }
                if (!rawTableDesc.isDraft()) {
                    official = rawTableDesc;
                    toDrop = false;
                }
                if (toDrop) {
                    deleteRaw(rawTable);
                }
            }
        }
        if (official != null && rename) {
            throw new BadRequestException(msg.getRAWTABLE_RENAME());
        }
        return youngerSelf;
    }

    public RawTableDesc updateRawTableToResourceStore(RawTableDesc desc, String projectName, boolean createNew)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String name = desc.getName();
        if (createNew) {
            createRawTableInstanceAndDesc(name, projectName, desc);
        } else {
            try {
                RawTableInstance raw = getRawTableManager().getRawTableInstance(name);

                if (raw == null) {
                    throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), name));
                }

                //raw table renaming is not allowed
                if (!raw.getRawTableDesc().getName().equalsIgnoreCase(desc.getName())) {
                    throw new BadRequestException(
                            String.format(msg.getRAW_DESC_RENAME(), desc.getName(), raw.getRawTableDesc().getName()));
                }

                desc = updateRawTableInstanceAndDesc(raw, desc, projectName, true);
            } catch (AccessDeniedException accessDeniedException) {
                throw new ForbiddenException(msg.getUPDATE_CUBE_NO_RIGHT());
            }
        }
        return desc;
    }

    public void deleteRawByUuid(String uuid, boolean isDraft) throws IOException {
        List<RawTableInstance> rawTables = getRawTableManager().listAllRawTables();
        for (RawTableInstance rawTable : rawTables) {
            RawTableDesc rawTableDesc = rawTable.getRawTableDesc();
            if (rawTableDesc.getUuid().equals(uuid)) {
                boolean sameStatus = rawTableDesc.isDraft() == isDraft;
                if (!isDraft || sameStatus) {
                    deleteRaw(rawTable);
                }
            }
        }
    }
}
