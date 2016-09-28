package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.WeakHashMap;

import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.HBaseResponse;
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

    private WeakHashMap<String, HBaseResponse> htableInfoCache = new WeakHashMap<>();

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
        createdRaw = getRawTableManager().createRawTableInstance(rawName, projectName, createdDesc, owner);
        accessService.init(createdRaw, AclPermission.ADMINISTRATION);

        ProjectInstance project = getProjectManager().getProject(projectName);
        accessService.inherit(createdRaw, project);

        return createdRaw;
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

    public static String getRawTableNameFromDesc(String descName) {
        if (descName.toLowerCase().endsWith(DESC_SUFFIX)) {
            return descName.substring(0, descName.toLowerCase().indexOf(DESC_SUFFIX));
        } else {
            return descName;
        }
    }
}
