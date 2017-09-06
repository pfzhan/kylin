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

package io.kyligence.kap.vube;

import static io.kyligence.kap.vube.VubeInstance.VUBE_RESOURCE_ROOT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeepClassWithMemberNames;

public class VubeManager implements IKeepClassWithMemberNames, IRealizationProvider {
    private static final Serializer<VubeInstance> VUBE_SERIALIZER = new JsonSerializer<VubeInstance>(
            VubeInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(io.kyligence.kap.vube.VubeManager.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, VubeManager> CACHE = new ConcurrentHashMap<KylinConfig, VubeManager>();

    public static VubeManager getInstance(KylinConfig config) {
        VubeManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (VubeManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new VubeManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init VubeManager from " + config, e);
            }
        }
    }

    private static void clearCache() {
        CACHE.clear();
    }

    public static void clearCache(KylinConfig kylinConfig) {
        if (kylinConfig != null)
            CACHE.remove(kylinConfig);
    }

    // ============================================================================

    private KylinConfig config;

    private CaseInsensitiveStringCache<VubeInstance> vubeMap;

    private VubeManager(KylinConfig config) throws IOException {
        logger.info("Initializing VubeManager with config " + config);
        this.config = config;
        this.vubeMap = new CaseInsensitiveStringCache<VubeInstance>(config, "vube");

        // touch lower level metadata before registering my listener
        reloadAllVubeInstance();
        Broadcaster.getInstance(config).registerListener(new VubeSyncListener(), "vube", "cube");
    }

    private class VubeSyncListener extends Broadcaster.Listener {

        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof VubeInstance) {
                    reloadVubeInstance(real.getName());
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            if ("vube".equals(entity)) {

                if (event == Broadcaster.Event.DROP)
                    vubeMap.removeLocal(cacheKey);
                else
                    reloadVubeInstance(cacheKey);

                for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.HYBRID2,
                        cacheKey)) {
                    broadcaster.notifyProjectSchemaUpdate(prj.getName());
                }
            } else if ("cube".equals(entity)) {
                String vubeName;

                if (cacheKey.contains("_version_")) {
                    vubeName = cacheKey.substring(0, cacheKey.indexOf("_version_"));
                } else {
                    vubeName = cacheKey;
                }

                if (event == Broadcaster.Event.DROP)
                    vubeMap.removeLocal(vubeName);
                else {
                    if (event == Broadcaster.Event.UPDATE) {
                        VubeInstance vube = getVubeInstance(vubeName);
                        CubeManager cubeManager = CubeManager.getInstance(config);
                        CubeInstance cube = cubeManager.getCube(cacheKey);

                        // New cube built successfully
                        if (vube != null && cube.getStatus() == RealizationStatusEnum.READY) {
                            if (vube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
                                // First time built, it should be ready
                                VubeUpdate update = new VubeUpdate(vube);
                                update.setStatus(RealizationStatusEnum.READY);
                                updateVube(update);
                            } else if (vube.getStatus() == RealizationStatusEnum.DISABLED) {
                                // Disabled vube should disable newly built cube
                                CubeUpdate cubeBuilder = new CubeUpdate(cube);
                                cubeBuilder.setStatus(RealizationStatusEnum.DISABLED);
                                cubeManager.updateCube(cubeBuilder);
                            }
                        }
                    }
                    if (vubeMap.get(vubeName) != null) {
                        reloadVubeInstance(vubeName);
                    }
                }

                for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.HYBRID2,
                        vubeName)) {
                    broadcaster.notifyProjectSchemaUpdate(prj.getName());
                }
            }
        }
    }

    private void reloadAllVubeInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(VUBE_RESOURCE_ROOT, ".json");

        vubeMap.clear();
        logger.debug("Loading vube from folder " + store.getReadableResourcePath(VUBE_RESOURCE_ROOT));

        for (String path : paths) {
            reloadVubeInstanceAt(path);
        }

        logger.debug("Loaded " + paths.size() + " Vube(s)");
    }

    public VubeInstance getVubeInstanceByCube(String cubeName) {
        VubeInstance result = null;

        for (VubeInstance vubeInstance : vubeMap.values()) {
            if (cubeName.startsWith(vubeInstance.getName())) {
                result = vubeInstance;
                break;
            }
        }

        return result;
    }

    private void reloadVubeInstance(String name) {
        reloadVubeInstanceAt(VubeInstance.concatResourcePath(name));
    }

    private synchronized VubeInstance reloadVubeInstanceAt(String path) {
        ResourceStore store = getStore();
        CubeManager cubeManager = CubeManager.getInstance(config);
        VubeInstance vubeInstance = null;
        try {
            vubeInstance = store.getResource(path, VubeInstance.class, VUBE_SERIALIZER);

            if (vubeInstance != null) {
                vubeInstance.setConfig(config);

                if (vubeInstance.getVersionedCubes() == null || vubeInstance.getVersionedCubes().size() == 0) {
                    throw new IllegalStateException("VubeInstance at " + path + "must contain cubes.");
                }

                if (StringUtils.isBlank(vubeInstance.getName()))
                    throw new IllegalStateException("VubeInstance name must not be blank, at " + path);

                List<CubeInstance> cubeList = new ArrayList<>();

                for (CubeInstance cube : vubeInstance.getVersionedCubes()) {
                    CubeInstance realCube = cubeManager.getCube(cube.getName());

                    if (realCube != null) {
                        cubeList.add(realCube);
                    }
                }

                vubeInstance.setVersionedCubes(cubeList);

                final String name = vubeInstance.getName();
                vubeMap.putLocal(name, vubeInstance);
            }

            return vubeInstance;
        } catch (Exception e) {
            logger.error("Error during load VubeInstance " + path, e);
            return null;
        }
    }

    public List<VubeInstance> listVubeInstances() {
        return new ArrayList<VubeInstance>(vubeMap.values());
    }

    public VubeInstance getVubeInstance(String name) {
        return vubeMap.get(name);
    }

    public VubeInstance dropVube(String vubeName) throws IOException {
        if (vubeName == null)
            throw new IllegalArgumentException("Vube name not given");

        VubeInstance vube = getVubeInstance(vubeName);

        if (vube == null) {
            throw new IllegalStateException("The vube named " + vubeName + " does not exist");
        }

        logger.info("Dropping vube '" + vubeName + "'");

        // remove vube and update cache
        getStore().deleteResource(vube.getResourcePath());
        vubeMap.remove(vube.getName());

        // delete vube from project
        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.HYBRID2, vubeName);

        return vube;
    }

    public VubeInstance createVube(String vubeName, CubeInstance cube, String projectName, String owner)
            throws IOException {
        logger.info("Creating vube with cube '" + projectName + "-->" + cube.getName() + "' from instance object. '");

        VubeInstance currentVube = getVubeInstance(vubeName);
        if (currentVube == null) {
            currentVube = VubeInstance.create(vubeName, cube);
        } else {
            throw new IllegalStateException("The vube named " + vubeName + "already exists");
        }

        currentVube.setOwner(owner);
        updateVube(currentVube);

        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.HYBRID2, vubeName, projectName,
                owner);

        return currentVube;
    }

    public VubeInstance updateVube(VubeUpdate update) throws IOException {
        if (update == null || update.getVubeInstance() == null)
            throw new IllegalStateException();

        VubeInstance vube = update.getVubeInstance();

        if (update.getOwner() != null) {
            vube.setOwner(update.getOwner());
        }

        if (update.getCost() >= 0) {
            vube.setCost(update.getCost());
        }

        if (update.getCubeToAdd() != null) {
            appendCube(vube, update.getCubeToAdd());
        }

        if (update.getCubesToUpdate() != null) {
            List<CubeInstance> cubeList = vube.getVersionedCubes();
            List<CubeInstance> updatedCubeList = new ArrayList<>();
            Map<String, CubeInstance> updateMap = new HashMap<>();

            if (cubeList.size() > 0) {
                for (CubeInstance newCube : update.getCubesToUpdate()) {
                    updateMap.put(newCube.getName(), newCube);
                }

                for (CubeInstance cube : cubeList) {
                    if (updateMap.get(cube.getName()) != null) {
                        CubeInstance cubeToUpdate = updateMap.get(cube.getName());

                        updatedCubeList.add(cubeToUpdate);
                    } else {
                        updatedCubeList.add(cube);
                    }
                }
            }

            vube.setVersionedCubes(updatedCubeList);
        }

        if (update.getStatus() != null) {
            vube.setStatus(update.getStatus());
        }

        try {
            updateVube(vube);
        } catch (IOException e) {
            logger.error("Fail to update vube " + vube.getName() + "'s metadata", e);
        }

        //this is a duplicate call to take care of scenarios where REST cache service unavailable
        ProjectManager.getInstance(vube.getConfig()).clearL2Cache();
        return vube;
    }

    private void updateVube(VubeInstance vube) throws IOException {
        synchronized (vube) {
            getStore().putResource(vube.getResourcePath(), vube, VUBE_SERIALIZER);
            vubeMap.put(vube.getName(), vube);
            ProjectManager.getInstance(vube.getConfig()).clearL2Cache();
        }
    }

    // Add a Cube to a Vube
    void appendCube(VubeInstance vube, CubeInstance appendedCube) throws IOException {
        for (CubeInstance cube : vube.getVersionedCubes()) {
            if (cube.getName().equals(appendedCube.getName())) {
                return;
            }
        }

        vube.getVersionedCubes().add(appendedCube);

        try {
            updateVube(vube);
        } catch (IOException e) {
            logger.error("Fail to update vube " + vube.getName() + "'s metadata", e);
        }
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.HYBRID2;
    }

    @Override
    public IRealization getRealization(String name) {
        return getVubeInstance(name);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
}
