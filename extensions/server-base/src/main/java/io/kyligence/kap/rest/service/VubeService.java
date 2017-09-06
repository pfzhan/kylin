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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableDescManager;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.vube.VubeInstance;
import io.kyligence.kap.vube.VubeManager;
import io.kyligence.kap.vube.VubeUpdate;

@Component("vubeService")
public class VubeService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(VubeService.class);

    protected Map<String, Set<String>> pendingCubes = new HashMap<>();

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<VubeInstance> listAllVubes(final String vubeName, final String projectName, final String modelName,
            boolean exactMatch) {
        List<VubeInstance> vubeInstances = null;
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;

        if (null == project) {
            vubeInstances = getVubeManager().listVubeInstances();
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            vubeInstances = listAllVubes(projectName);
            aclEvaluate.hasProjectReadPermission(project);
        }

        List<VubeInstance> filterModelVubes = new ArrayList<>();

        if (modelName != null) {
            for (VubeInstance vubeInstance : vubeInstances) {
                CubeInstance cube = vubeInstance.getLatestCube();
                boolean isVubeMatch = false;

                if (cube != null) {
                    isVubeMatch = cube.getDescriptor().getModelName().toLowerCase().equals(modelName.toLowerCase());
                }
                if (isVubeMatch) {
                    filterModelVubes.add(vubeInstance);
                }
            }
        } else {
            filterModelVubes = vubeInstances;
        }

        List<VubeInstance> filterVubes = new ArrayList<VubeInstance>();
        for (VubeInstance vubeInstance : filterModelVubes) {
            boolean isCubeMatch = (null == vubeName)
                    || (!exactMatch && vubeInstance.getName().toLowerCase().contains(vubeName.toLowerCase()))
                    || (exactMatch && vubeInstance.getName().toLowerCase().equals(vubeName.toLowerCase()));

            if (isCubeMatch) {
                filterVubes.add(vubeInstance);
            }
        }

        return filterVubes;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public VubeInstance createVube(String vubeName, ProjectInstance project, CubeInstance originCube)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (getVubeManager().getVubeInstance(vubeName) != null) {
            throw new BadRequestException(String.format(msg.getVUBE_ALREADY_EXIST(), vubeName));
        }

        VubeInstance createdVube;
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();

        createdVube = getVubeManager().createVube(vubeName, originCube, project.getName(), owner);
        accessService.init(createdVube, AclPermission.ADMINISTRATION);

        accessService.inherit(createdVube, project);

        return createdVube;
    }

    public VubeInstance updateVube(VubeInstance vube, VubeUpdate update) throws IOException {
        ProjectManager projectManager = getProjectManager();
        if (update.getProject() != null && !isVubeInProject(update.getProject(), vube)) {
            aclEvaluate.hasProjectWritePermission(projectManager.getProject(update.getProject()));

            String owner = SecurityContextHolder.getContext().getAuthentication().getName();
            ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.HYBRID2,
                    vube.getName(), update.getProject(), owner);

            accessService.inherit(vube, newProject);
        }

        vube.getLatestCube().getName();

        return getVubeManager().updateVube(update);
    }

    public VubeInstance updateCube(VubeInstance vube, String cubeName) throws IOException {
        VubeUpdate update = new VubeUpdate(vube);
        update.setCubesToUpdate(getCubeManager().getCube(cubeName));

        return getVubeManager().updateVube(update);
    }

    public VubeInstance updateNotificationSetting(VubeInstance vube, List<String> notifyList,
            List<String> statusNeedNotify) throws IOException {
        CubeInstance cube = vube.getLatestCube();

        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
            CubeDesc desc = realCube.getDescriptor();
            desc.setNotifyList(notifyList);
            desc.setStatusNeedNotify(statusNeedNotify);
            getCubeDescManager().updateCubeDesc(desc);
        }

        return vube;
    }

    public VubeInstance updatePartitionDateStart(VubeInstance vube, long date) throws IOException {
        CubeInstance cube = vube.getLatestCube();

        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
            CubeDesc desc = realCube.getDescriptor();
            desc.setPartitionDateStart(date);
            getCubeDescManager().updateCubeDesc(desc);
        }

        return vube;
    }

    public VubeInstance updateMergeRanges(VubeInstance vube, Long[] timeRanges) throws IOException {
        CubeInstance cube = vube.getLatestCube();

        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
            RawTableDescManager rawTableDescManager = RawTableDescManager.getInstance(vube.getConfig());
            CubeDesc cubeDesc = realCube.getDescriptor();
            RawTableDesc rawTableDesc = rawTableDescManager.getRawTableDesc(cube.getName());
            cubeDesc.setAutoMergeTimeRanges(ArrayUtils.toPrimitive(timeRanges));
            getCubeDescManager().updateCubeDesc(cubeDesc);

            if (rawTableDesc != null) {
                rawTableDesc.setAutoMergeTimeRanges(ArrayUtils.toPrimitive(timeRanges));
                rawTableDescManager.updateRawTableDesc(rawTableDesc);
            }
        }

        return vube;
    }

    public VubeInstance updateRetentionThreshold(VubeInstance vube, Long threshold) throws IOException {
        CubeInstance cube = vube.getLatestCube();

        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
            CubeDesc cubeDesc = realCube.getDescriptor();
            cubeDesc.setRetentionRange(threshold);
            getCubeDescManager().updateCubeDesc(cubeDesc);
        }

        return vube;
    }

    public VubeInstance updateCubeEngine(VubeInstance vube, int cubeEngine) throws IOException {
        CubeInstance cube = vube.getLatestCube();

        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
            CubeDesc cubeDesc = realCube.getDescriptor();
            cubeDesc.setEngineType(cubeEngine);
            getCubeDescManager().updateCubeDesc(cubeDesc);
        }

        return vube;
    }

    public VubeInstance updateCubeConfig(VubeInstance vube, Map<String, String> cubeConfig) throws IOException {
        CubeInstance cube = vube.getLatestCube();

        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
            CubeDesc cubeDesc = realCube.getDescriptor();
            Map<String, String> overrideKylinProps = cubeDesc.getOverrideKylinProps();

            for (String key : cubeConfig.keySet()) {
                if (overrideKylinProps.containsKey(key)) {
                    overrideKylinProps.put(key, cubeConfig.get(key));
                }
            }
            getCubeDescManager().updateCubeDesc(cubeDesc);
        }

        return vube;
    }

    public int getSegmentStorageType(CubeSegment segment) {
        return getCubeManager().getCube(segment.getCubeInstance().getName()).getDescriptor().getStorageType();
    }

    public void deleteVube(VubeInstance vube) throws IOException {
        CubeInstance cube = vube.getLatestCube();
        if (cube != null) {
            CubeInstance realCube = getCubeManager().getCube(cube.getName());
            aclEvaluate.hasProjectWritePermission(realCube.getProjectInstance());
        }
        getVubeManager().dropVube(vube.getName());
        accessService.clean(vube, true);
    }

    public VubeInstance getVubeInstance(String vubeName) {
        VubeInstance vubeInstance = getVubeManager().getVubeInstance(vubeName);
        return vubeInstance;
    }

    public VubeManager getVubeManager() {
        return VubeManager.getInstance(getConfig());
    }

    protected boolean isVubeInProject(String projectName, VubeInstance target) {
        ProjectManager projectManager = getProjectManager();
        ProjectInstance project = projectManager.getProject(projectName);
        if (project == null) {
            return false;
        }
        for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
            if (projectDataModel.getType() == RealizationType.HYBRID2) {
                VubeInstance vube = getVubeManager().getVubeInstance(projectDataModel.getRealization());
                if (vube == null) {
                    logger.error("Project " + projectName + " contains realization " + projectDataModel.getRealization()
                            + " which is not found by VubeManager");
                    continue;
                }
                if (vube.equals(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<VubeInstance> listAllVubes(String projectName) {
        ProjectManager projectManager = getProjectManager();
        ProjectInstance project = projectManager.getProject(projectName);

        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
            return Collections.emptyList();
        } else {
            aclEvaluate.hasProjectReadPermission(project);
        }

        ArrayList<VubeInstance> result = new ArrayList<>();
        for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
            if (projectDataModel.getType() == RealizationType.HYBRID2) {
                VubeInstance vube = getVubeManager().getVubeInstance(projectDataModel.getRealization());
                if (vube != null)
                    result.add(vube);
                else
                    logger.error("Vube instance " + projectDataModel.getRealization() + " is failed to load");
            }
        }
        return result;
    }

    public Segments<CubeSegment> listSegments(final String vubeName, final String projectName, final String version) {
        List<VubeInstance> vubeInstances = new LinkedList<>();
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;
        Segments<CubeSegment> segments = new Segments<>();

        if (vubeName != null) {
            VubeInstance vube = getVubeInstance(vubeName);

            if (vube != null) {
                vubeInstances.add(vube);
            }
        } else if (project == null) {
            vubeInstances = getVubeManager().listVubeInstances();
        } else {
            vubeInstances = listAllVubes(projectName);
        }

        if (version == null || version.length() == 0) {
            for (VubeInstance vubeInstance : vubeInstances) {
                segments.addAll(vubeInstance.getAllSegments());
            }
        } else {
            for (VubeInstance vubeInstance : vubeInstances) {
                segments.addAll(vubeInstance.getSegments(version));
            }
        }

        return segments;
    }

    public CubeInstance getLatestCube(String vubeName) {
        VubeInstance vube = getVubeManager().getVubeInstance(vubeName);
        CubeInstance cube = null;

        if (vube != null) {
            cube = getCubeManager().getCube(vube.getLatestCube().getName());
        }

        return cube;
    }

    public CubeInstance getCubeWithVersion(String projectName, String vubeName, String version) {
        VubeInstance vube = getVubeManager().getVubeInstance(vubeName);
        CubeInstance cube = null;

        if (vube != null) {
            if (version == null || version.length() == 0) {
                cube = getCubeManager().getCube(getLatestCube(vubeName).getName());
            } else if (version.equals("version_0")) {
                cube = getCubeManager().getCube(vubeName);
            } else {
                cube = getCubeManager().getCube(vubeName + "_" + version);
            }
        }

        return cube;
    }

    public void addPendingCube(String vubeName, String cubeName) {
        if (pendingCubes.get(cubeName) == null) {
            pendingCubes.put(vubeName, new HashSet<String>());
        }

        Set<String> pendingList = pendingCubes.get(vubeName);
        pendingList.add(cubeName);
        pendingCubes.put(vubeName, pendingList);
    }

    public Set<String> getPendingCubes(String vubeName) {
        return pendingCubes.get(vubeName);
    }

    public Set<String> clearPendingCubes(String vubeName) {
        return pendingCubes.remove(vubeName);
    }

    public List<String> listAllVubeNames() {
        ArrayList<String> result = new ArrayList<>();
        List<VubeInstance> vubeInstances = getVubeManager().listVubeInstances();

        for (VubeInstance vube : vubeInstances) {
            result.add(vube.getName());
        }
        return result;
    }
}
