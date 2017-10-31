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

package io.kyligence.kap.cube.model;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceNotFoundException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.cuboid.CuboidManager;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.cube.model.validation.NCubePlanValidator;

public class NCubePlanManager implements IKeepNames {
    public static final Serializer<NCubePlan> CUBE_PLAN_SERIALIZER = new JsonSerializer<>(NCubePlan.class);
    public static final int CUBOID_DESC_ID_STEP = 1000;
    public static final int CUBOID_LAYOUT_ID_STEP = 1;

    private static final Logger logger = LoggerFactory.getLogger(NCubePlanManager.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, NCubePlanManager> CACHE = new ConcurrentHashMap<>();
    private KylinConfig config;
    // name ==> NCubePlan
    private CaseInsensitiveStringCache<NCubePlan> cubePlanMap;

    // ============================================================================

    private NCubePlanManager(KylinConfig config) throws IOException {
        logger.info("Initializing NCubePlanManager with config " + config);
        this.config = config;
        this.cubePlanMap = new CaseInsensitiveStringCache<>(config, "cube_plan");

        // touch lower level metadata before registering my listener
        reloadAllCubePlans();
        Broadcaster.getInstance(config).registerListener(new CubePlanSyncListener(), "cube_plan");
    }

    public static NCubePlanManager getInstance(KylinConfig config) {
        NCubePlanManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (NCubePlanManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new NCubePlanManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init NCubePlanManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    public NCubePlan getCubePlan(String name) {
        return cubePlanMap.get(name);
    }

    public List<NCubePlan> listAllCubePlans() {
        return Lists.newArrayList(cubePlanMap.values());
    }

    public NCubePlan reloadCubePlanLocal(String name) {
        // Broken NCubePlan is not allowed to be saved and broadcast.
        NCubePlan ndesc;
        String path = NCubePlan.concatResourcePath(name);
        try {
            ndesc = loadCubePlan(path, false);
            if (ndesc == null) {
                throw new ResourceNotFoundException("No cube plan found at " + path);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load cube plan at " + path, ex);
        }

        cubePlanMap.putLocal(ndesc.getName(), ndesc);
        clearCuboidCache(ndesc.getName());

        // if related NDataflow is in DESCBROKEN state before, change it back to DISABLED
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config);
        for (NDataflow df : dataflowManager.getDataflowsByCubePlan(name)) {
            if (df.getStatus() == RealizationStatusEnum.DESCBROKEN) {
                dataflowManager.reloadDataflowLocal(df.getName());
            }
        }

        return ndesc;
    }

    private NCubePlan loadCubePlan(String path, boolean allowBroken) throws IOException {
        ResourceStore store = getStore();
        NCubePlan cubePlan = store.getResource(path, NCubePlan.class, CUBE_PLAN_SERIALIZER);
        if (cubePlan == null)
            throw new ResourceNotFoundException("No NCubePlan found at " + path);

        try {
            cubePlan.init(config);
        } catch (Exception e) {
            logger.warn("Broken NCubePlan at " + path, e);
            cubePlan.addError(e.getMessage());
        }

        if (!allowBroken && !cubePlan.getError().isEmpty()) {
            throw new IllegalStateException("NCubePlan at " + path + " has issues: " + cubePlan.getErrorMsg());
        }

        return cubePlan;
    }

    public NCubePlan createCubePlan(NCubePlan cubePlan) throws IOException {
        if (cubePlan.getUuid() == null || cubePlan.getName() == null)
            throw new IllegalArgumentException();
        if (cubePlanMap.containsKey(cubePlan.getName()))
            throw new IllegalArgumentException("NCubePlan '" + cubePlan.getName() + "' already exists");

        try {
            cubePlan.init(config);
        } catch (Exception e) {
            logger.warn("Broken cube plan " + cubePlan, e);
            cubePlan.addError(e.getMessage());
        }

        // Check base validation
        if (!cubePlan.getError().isEmpty()) {
            return cubePlan;
        }
        // Semantic validation
        NCubePlanValidator validator = new NCubePlanValidator();
        ValidateContext context = validator.validate(cubePlan);
        if (!context.ifPass()) {
            return cubePlan;
        }

        String path = cubePlan.getResourcePath();
        getStore().putResource(path, cubePlan, CUBE_PLAN_SERIALIZER);
        cubePlanMap.put(cubePlan.getName(), cubePlan);

        return cubePlan;
    }

    public NCubePlan addCuboidDesc(NCubePlan cubePlan, NCuboidDesc cuboidDesc) throws IOException {
        NCuboidDesc lastCuboidDesc = cubePlan.getLastCuboidDesc();
        cuboidDesc.setId(lastCuboidDesc.getId() + CUBOID_DESC_ID_STEP);
        cubePlan.getCuboids().add(cuboidDesc);

        return updateCubePlan(cubePlan);
    }

    public NCubePlan addCuboidLayout(NCubePlan cubePlan, int cuboidDescId, NCuboidLayout cuboidLayout)
            throws IOException {
        NCuboidDesc targetCuboidDesc = cubePlan.getCuboidDesc(cuboidDescId);
        Preconditions.checkNotNull(targetCuboidDesc, "CuboidDesc %d not found.", cuboidDescId);

        NCuboidLayout lastLayout = targetCuboidDesc.getLastLayout();
        cuboidLayout.setId(lastLayout.getId() + CUBOID_LAYOUT_ID_STEP);
        targetCuboidDesc.getLayouts().add(cuboidLayout);

        return updateCubePlan(cubePlan);
    }

    public NCubePlan updateCubePlan(NCubePlan cubePlan) throws IOException {
        if (cubePlan.getUuid() == null || cubePlan.getName() == null)
            throw new IllegalArgumentException();

        String name = cubePlan.getName();
        if (!cubePlanMap.containsKey(name))
            throw new IllegalArgumentException("NCubePlan '" + name + "' does not exist.");

        try {
            cubePlan.init(config);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + cubePlan, e);
            cubePlan.addError(e.getMessage());
            return cubePlan;
        }

        // Save Source
        String path = cubePlan.getResourcePath();
        getStore().putResource(path, cubePlan, CUBE_PLAN_SERIALIZER);

        // Reload the CubeDesc
        NCubePlan ndesc = loadCubePlan(path, false);
        // Here replace the old one
        cubePlanMap.put(ndesc.getName(), cubePlan);

        return ndesc;
    }

    // remove cubePlan
    public void removeCubePlan(NCubePlan cubePlan) throws IOException {
        String path = cubePlan.getResourcePath();
        getStore().deleteResource(path);
        cubePlanMap.remove(cubePlan.getName());
        clearCuboidCache(cubePlan.getName());
    }

    // remove cubeDesc
    public void removeLocalCubePlan(String name) throws IOException {
        cubePlanMap.removeLocal(name);
        clearCuboidCache(name);
    }

    private void reloadAllCubePlans() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading NCubePlan from folder "
                + store.getReadableResourcePath(NCubePlan.CUBE_PLAN_RESOURCE_ROOT));

        cubePlanMap.clear();

        List<String> paths = store.collectResourceRecursively(NCubePlan.CUBE_PLAN_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            NCubePlan desc = null;
            try {
                desc = loadCubePlan(path, true);
            } catch (Exception e) {
                logger.error("Error during load NCubePlan, skipping " + path, e);
                continue;
            }

            if (!path.equals(desc.getResourcePath())) {
                logger.error(
                        "Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (cubePlanMap.containsKey(desc.getName())) {
                logger.error("Dup NCubePlan name '" + desc.getName() + "' on path " + path);
                continue;
            }

            cubePlanMap.putLocal(desc.getName(), desc);
        }

        logger.info("Loaded " + cubePlanMap.size() + " NCubePlan(s)");
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    private void clearCuboidCache(String name) {
        // avoid calling CubeDesc.getInitialCuboidScheduler() for late initializing CuboidScheduler
        CuboidManager.getInstance(config).clearCache(name);
    }

    private class CubePlanSyncListener extends Broadcaster.Listener {

        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
            //            Cuboid.clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof NDataflow) {
                    String planName = ((NDataflow) real).getCubePlanName();
                    reloadCubePlanLocalQuietly(planName);
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String planName = cacheKey;
            NCubePlan cubePlan = getCubePlan(planName);
            String modelName = cubePlan == null ? null : cubePlan.getModelName();

            if (event == Broadcaster.Event.DROP)
                removeLocalCubePlan(planName);
            else
                reloadCubePlanLocalQuietly(planName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(modelName)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
        
        private void reloadCubePlanLocalQuietly(String name) {
            try {
                reloadCubePlanLocal(name);
            } catch (ResourceNotFoundException ex) { // ResourceNotFoundException is normal a resource is deleted
                logger.warn("Failed to load cube plan " + name + ", brief: " + ex.toString());
            }
        }
    }

}
