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

import static io.kyligence.kap.cube.raw.RawTableDesc.STATUS_DRAFT;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
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
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableUpdate;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

/**
 * Created by luwei on 17-4-29.
 */
@Component("rawTableServiceV2")
public class RawTableServiceV2 extends RawTableService {

    private static final Logger logger = LoggerFactory.getLogger(RawTableServiceV2.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or " + Constant.ACCESS_HAS_ROLE_MODELER)
    public RawTableInstance createRawTableInstanceAndDesc(String rawName, String projectName, RawTableDesc desc) throws IOException {
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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#raw, 'ADMINISTRATION') or hasPermission(#raw, 'MANAGEMENT')")
    public RawTableDesc updateRawTableInstanceAndDesc(RawTableInstance raw, RawTableDesc desc, String newProjectName, boolean forceUpdate) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(raw.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(msg.getRAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB());
        }

        RawTableDesc updatedRawTableDesc = getRawTableDescManager().updateRawTableDesc(desc);
        ProjectManager projectManager = getProjectManager();
        if (!isRawTableInProject(newProjectName, raw)) {
            String owner = SecurityContextHolder.getContext().getAuthentication().getName();
            ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.CUBE, raw.getName(), newProjectName, owner);
            accessService.inherit(raw, newProject);
        }

        return updatedRawTableDesc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteRaw(RawTableInstance raw) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(raw.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_HAS_RUNNING_JOB(), raw.getName()));
        }
        int rawNum = getRawTableManager().getRawTablesByDesc(raw.getDescName()).size();
        getRawTableManager().dropRawTableInstance(raw.getName(), rawNum == 1);//only delete cube desc when no other cube is using it
        accessService.clean(raw, true);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
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

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(raw.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
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

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
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
        KapMessage msg = KapMsgPicker.getMsg();

        boolean createNew = false;
        String rawTableName = desc.getName();
        String originName = null;   // for draft rename check
        if (desc.getUuid() != null) {
            originName = getNameByUuid(desc.getUuid());
        }

        if (!isDraft) { // save as official raw table
            if (desc.getStatus() != null && desc.getStatus().equals(STATUS_DRAFT)) {  // from draft
                if (originName == null) {
                    throw new BadRequestException(msg.getORIGIN_RAWTABLE_NOT_FOUND());
                }
                originName = originName.substring(0, originName.lastIndexOf("_draft"));
                if (!originName.equals(rawTableName)) {     // if rename draft
                    RawTableDesc parentDesc = getRawTableDescManager().getRawTableDesc(originName);
                    if (parentDesc == null) {   // only allow rename when official raw table has not been saved
                        createNew = true;
                        deleteRawTableByUuid(desc.getUuid());
                        desc.setStatus(null);
                        desc.setLastModified(0);
                        desc.setUuid(UUID.randomUUID().toString());
                    } else {
                        throw new BadRequestException(msg.getRAWTABLE_RENAME());
                    }
                } else {    // without rename draft
                    desc.setStatus(null);
                    RawTableDesc parentDesc = getRawTableDescManager().getRawTableDesc(originName);
                    if (parentDesc == null) {   // official raw table doesn't exist, create new one
                        createNew = true;
                        desc.setLastModified(0);
                        desc.setUuid(UUID.randomUUID().toString());
                    } else {    // update existing
                        desc.setLastModified(parentDesc.getLastModified());
                        desc.setUuid(parentDesc.getUuid());
                    }
                }
            } else {    // from official
                if (originName == null) {   // official raw table doesn't exist, create new one
                    createNew = true;
                    desc.setLastModified(0);
                    desc.setUuid(UUID.randomUUID().toString());
                } else {
                    if (!originName.equals(rawTableName)) {    // do not allow official raw table rename
                        throw new BadRequestException(msg.getRAWTABLE_RENAME());
                    }
                }
            }
        } else {    // save as draft raw table
            if (desc.getStatus() == null) {    // from official
                rawTableName += "_draft";
                desc.setName(rawTableName);
                desc.setStatus(STATUS_DRAFT);
                RawTableDesc draftDesc = getRawTableDescManager().getRawTableDesc(rawTableName);
                if (draftDesc == null) {
                    createNew = true;
                    desc.setLastModified(0);
                    desc.setUuid(UUID.randomUUID().toString());
                } else if (draftDesc.getStatus() != null && draftDesc.getStatus().equals(STATUS_DRAFT)) {   // update existing
                        desc.setLastModified(draftDesc.getLastModified());
                        desc.setUuid(draftDesc.getUuid());
                } else {    // already exist an official draft with name ends with '_draft'
                    throw new BadRequestException(String.format(msg.getNON_DRAFT_RAWTABLE_ALREADY_EXIST(), rawTableName));
                }
            } else if (desc.getStatus().equals(STATUS_DRAFT)) {    // from draft
                if (originName == null) {
                    throw new BadRequestException(msg.getORIGIN_RAWTABLE_NOT_FOUND());
                }
                originName = originName.substring(0, originName.lastIndexOf("_draft"));
                if (!originName.equals(rawTableName)) {    // if rename draft
                    RawTableDesc parentDesc = getRawTableDescManager().getRawTableDesc(originName);
                    if (parentDesc == null) {   // only allow rename when official raw table has not been saved
                        createNew = true;
                        deleteRawTableByUuid(desc.getUuid());
                        rawTableName += "_draft";
                        desc.setName(rawTableName);
                        desc.setStatus(STATUS_DRAFT);
                        desc.setLastModified(0);
                        desc.setUuid(UUID.randomUUID().toString());
                    } else {
                        throw new BadRequestException(msg.getRAWTABLE_RENAME());
                    }
                } else {    // without rename draft
                    rawTableName += "_draft";
                    desc.setName(rawTableName);
                }
            }
        }
        return createNew;
    }

    public String getNameByUuid(String uuid) {
        List<RawTableInstance> rawTables = getRawTableManager().listAllRawTables();
        for (RawTableInstance rawTable : rawTables) {
            if (rawTable.getRawTableDesc().getUuid().equals(uuid)) {
                return rawTable.getName();
            }
        }
        return null;
    }

    public void deleteRawTableByUuid(String uuid) throws IOException {
        List<RawTableInstance> rawTables = getRawTableManager().listAllRawTables();
        for (RawTableInstance rawTable : rawTables) {
            if (rawTable.getRawTableDesc().getUuid().equals(uuid)) {
                deleteRaw(rawTable);
            }
        }
    }

    public RawTableDesc updateRawTableToResourceStore(RawTableDesc desc, String projectName, boolean createNew, boolean isDraft) throws IOException {
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
                    throw new BadRequestException(String.format(msg.getRAW_DESC_RENAME(), desc.getName(), raw.getRawTableDesc().getName()));
                }

                desc = updateRawTableInstanceAndDesc(raw, desc, projectName, true);
            } catch (AccessDeniedException accessDeniedException) {
                throw new ForbiddenException(msg.getUPDATE_CUBE_NO_RIGHT());
            }
        }

        if (!isDraft) {
            RawTableInstance draftRawTable = getRawTableManager().getRawTableInstance(name + "_draft");
            if (null != draftRawTable && draftRawTable.getRawTableDesc().getStatus() != null && draftRawTable.getRawTableDesc().getStatus().equals(STATUS_DRAFT)) {
                deleteRaw(draftRawTable);
            }
        }
        return desc;
    }
}
