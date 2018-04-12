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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.cube.model.validation.NCubePlanValidator;

public class NCubePlanManager implements IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NCubePlanManager.class);

    public static final long CUBOID_DESC_ID_STEP = 1000L;
    public static final long CUBOID_LAYOUT_ID_STEP = 1L;

    public static NCubePlanManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NCubePlanManager.class);
    }

    // called by reflection
    static NCubePlanManager newInstance(KylinConfig config, String project) throws IOException {
        return new NCubePlanManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    // name ==> NCubePlan
    private CaseInsensitiveStringCache<NCubePlan> cubePlanMap;
    private CachedCrudAssist<NCubePlan> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock cubePlanMapLock = new AutoReadWriteLock();

    private NCubePlanManager(KylinConfig cfg, final String project) throws IOException {
        logger.info("Initializing NCubePlanManager with config " + config);
        this.config = cfg;
        this.project = project;
        this.cubePlanMap = new CaseInsensitiveStringCache<>(config, "cube_plan");
        String resourceRootPath = "/" + project + NCubePlan.CUBE_PLAN_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NCubePlan>(getStore(), resourceRootPath, NCubePlan.class, cubePlanMap) {
            @Override
            protected NCubePlan initEntityAfterReload(NCubePlan cubePlan, String resourceName) {
                try {
                    cubePlan.setProject(project);
                    cubePlan.initAfterReload(config);
                } catch (Exception e) {
                    logger.warn("Broken NCubePlan " + resourceName, e);
                    cubePlan.addError(e.getMessage());
                }
                return cubePlan;
            }
        };
        this.crud.setCheckCopyOnWrite(true);

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new CubePlanSyncListener(), "cube_plan");
    }

    private class CubePlanSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : NProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof NDataflow) {
                    String planName = ((NDataflow) real).getCubePlanName();
                    try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
                        crud.reloadQuietly(planName);
                    }
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String planName = cacheKey;
            NCubePlan cubePlan = getCubePlan(planName);
            String modelName = cubePlan == null ? null : cubePlan.getModelName();

            try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
                if (event == Broadcaster.Event.DROP)
                    cubePlanMap.removeLocal(planName);
                else
                    crud.reloadQuietly(planName);
            }

            for (ProjectInstance prj : NProjectManager.getInstance(config).findProjectsByModel(modelName)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    public NCubePlan copy(NCubePlan plan) {
        return crud.copyBySerialization(plan);
    }

    public NCubePlan getCubePlan(String name) {
        try (AutoLock lock = cubePlanMapLock.lockForRead()) {
            return cubePlanMap.get(name);
        }
    }

    public List<NCubePlan> listAllCubePlans() {
        try (AutoLock lock = cubePlanMapLock.lockForRead()) {
            return Lists.newArrayList(cubePlanMap.values());
        }
    }

    public NCubePlan createCubePlan(NCubePlan cubePlan) throws IOException {
        try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
            if (cubePlan.getUuid() == null || cubePlan.getName() == null)
                throw new IllegalArgumentException();
            if (cubePlanMap.containsKey(cubePlan.getName()))
                throw new IllegalArgumentException("NCubePlan '" + cubePlan.getName() + "' already exists");

            try {
                // init the cube plan if not yet
                if (cubePlan.getConfig() == null)
                    cubePlan.initAfterReload(config);
            } catch (Exception e) {
                logger.warn("Broken cube plan " + cubePlan, e);
                cubePlan.addError(e.getMessage());
            }

            // Check base validation
            if (!cubePlan.getError().isEmpty()) {
                throw new IllegalArgumentException(cubePlan.getErrorMsg());
            }
            // Semantic validation
            NCubePlanValidator validator = new NCubePlanValidator();
            ValidateContext context = validator.validate(cubePlan);
            if (!context.ifPass()) {
                throw new IllegalArgumentException(cubePlan.getErrorMsg());
            }

            NCubePlan saved = crud.save(cubePlan);

//            NProjectManager.getInstance(config).moveRealizationToProject()
            return saved;
        }
    }

    public interface NCubePlanUpdater {
        void modify(NCubePlan copyForWrite);
    }

    public NCubePlan updateCubePlan(String cubePlanName, NCubePlanUpdater updater) throws IOException {
        try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
            NCubePlan cached = getCubePlan(cubePlanName);
            NCubePlan copy = copy(cached);
            updater.modify(copy);
            return updateCubePlan(copy);
        }
    }

    // use the NCubePlanUpdater instead
    @Deprecated
    public NCubePlan updateCubePlan(NCubePlan cubePlan) throws IOException {
        try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
            if (cubePlan.isCachedAndShared())
                throw new IllegalStateException();

            if (cubePlan.getUuid() == null || cubePlan.getName() == null)
                throw new IllegalArgumentException();

            String name = cubePlan.getName();
            if (!cubePlanMap.containsKey(name))
                throw new IllegalArgumentException("NCubePlan '" + name + "' does not exist.");

            try {
                // init the cube plan if not yet
                if (cubePlan.getConfig() == null)
                    cubePlan.initAfterReload(config);
            } catch (Exception e) {
                logger.warn("Broken cube desc " + cubePlan, e);
                cubePlan.addError(e.getMessage());
                throw new IllegalArgumentException(cubePlan.getErrorMsg());
            }

            return crud.save(cubePlan);
        }
    }

    // remove cubePlan
    public void removeCubePlan(NCubePlan cubePlan) throws IOException {
        try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
            crud.delete(cubePlan);
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

}
