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
import java.util.Map;
import java.util.Set;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.cube.model.validation.NCubePlanValidator;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class NCubePlanManager implements IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NCubePlanManager.class);

    public static final long CUBOID_DESC_ID_STEP = 1000L;
    public static final long CUBOID_LAYOUT_ID_STEP = 1L;

    public static final String NCUBE_PLAN_ENTITY_NAME = "cube_plan";

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
        this.cubePlanMap = new CaseInsensitiveStringCache<>(config, project, NCUBE_PLAN_ENTITY_NAME);
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
        Broadcaster.getInstance(config).registerListener(new CubePlanSyncListener(), project, NCUBE_PLAN_ENTITY_NAME);
    }

    public List<NCubePlan> findMatchingCubePlan(String modelName, String project, KylinConfig kylinConfig) {
        List<NCubePlan> matchingCubePlans = Lists.newArrayList();
        Set<IRealization> realizations = NProjectManager.getInstance(kylinConfig).listAllRealizations(project);
        for (IRealization realization : realizations) {
            if (realization instanceof NDataflow) {
                NCubePlan cubePlan = ((NDataflow) realization).getCubePlan();
                if (cubePlan.getModelName().equals(modelName)) {
                    matchingCubePlans.add(cubePlan);
                }
            }
        }

        return matchingCubePlans;
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
            String prj = cubePlan == null ? null : cubePlan.getProject();

            try (AutoLock lock = cubePlanMapLock.lockForWrite()) {
                if (event == Broadcaster.Event.DROP)
                    cubePlanMap.removeLocal(planName);
                else
                    crud.reloadQuietly(planName);

            }

            broadcaster.notifyProjectSchemaUpdate(prj);
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

            return crud.save(cubePlan);
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

    public void removeLayouts(NCubePlan cubePlan, Set<Long> cuboidLayoutIds,
            Predicate2<NCuboidLayout, NCuboidLayout> comparator) {
        val cuboidMap = Maps.newHashMap(cubePlan.getCuboidMap());
        val toRemovedMap = Maps.<NCuboidIdentifier, List<NCuboidLayout>> newHashMap();
        for (Map.Entry<NCuboidIdentifier, NCuboidDesc> cuboidDescEntry : cuboidMap.entrySet()) {
            if (cuboidDescEntry.getValue().isRuleBased()) {
                continue;
            }
            val layouts = cuboidDescEntry.getValue().getLayouts();
            val filteredLayouts = Lists.<NCuboidLayout> newArrayList();
            for (NCuboidLayout layout : layouts) {
                if (cuboidLayoutIds.contains(layout.getId())) {
                    filteredLayouts.add(layout);
                }
            }

            toRemovedMap.put(cuboidDescEntry.getKey(), filteredLayouts);
        }

        removeLayouts(cubePlan, toRemovedMap, comparator);
    }

    /**
     * remove useless layouts from cubePlan without shared
     * this method will not persist cubePlan entity
     *
     * @param cubePlan        the cubePlan's isCachedAndShared must be false
     * @param cuboidLayoutMap the layouts to be removed, group by cuboid's identify
     * @param comparator      compare if two layouts is equal
     */
    public void removeLayouts(NCubePlan cubePlan, Map<NCuboidIdentifier, List<NCuboidLayout>> cuboidLayoutMap,
            Predicate2<NCuboidLayout, NCuboidLayout> comparator) {
        removeLayouts(cubePlan, cuboidLayoutMap, null, comparator);
    }

    public void removeLayouts(NCubePlan cubePlan, Map<NCuboidIdentifier, List<NCuboidLayout>> cuboids,
            Predicate<NCuboidLayout> isSkip, Predicate2<NCuboidLayout, NCuboidLayout> equal) {
        Map<NCuboidIdentifier, NCuboidDesc> originalCuboidsMap = cubePlan.getCuboidMap();
        for (Map.Entry<NCuboidIdentifier, List<NCuboidLayout>> cuboidEntity : cuboids.entrySet()) {
            NCuboidIdentifier cuboidKey = cuboidEntity.getKey();
            NCuboidDesc originalCuboid = originalCuboidsMap.get(cuboidKey);
            if (originalCuboid == null) {
                continue;
            }
            removeLayoutsInCuboid(originalCuboid, cuboidEntity.getValue(), isSkip, equal);
            if (originalCuboid.getLayouts().isEmpty()) {
                originalCuboidsMap.remove(cuboidKey);
            }
        }

        cubePlan.setCuboids(Lists.newArrayList(originalCuboidsMap.values()));
    }

    private void removeLayoutsInCuboid(NCuboidDesc originalCuboid, List<NCuboidLayout> deprecatedLayouts,
            Predicate<NCuboidLayout> isSkip, Predicate2<NCuboidLayout, NCuboidLayout> equal) {
        List<NCuboidLayout> toRemoveLayouts = Lists.newArrayList();
        for (NCuboidLayout cuboidLayout : deprecatedLayouts) {
            if (isSkip != null && isSkip.apply(cuboidLayout)) {
                continue;
            }
            NCuboidLayout toRemoveLayout = null;
            for (NCuboidLayout originalLayout : originalCuboid.getLayouts()) {
                if (equal.apply(originalLayout, cuboidLayout)) {
                    toRemoveLayout = originalLayout;
                    break;
                }
            }
            if (toRemoveLayout != null) {
                toRemoveLayouts.add(toRemoveLayout);
            }
        }
        logger.debug("to remove {}", toRemoveLayouts);
        originalCuboid.getLayouts().removeAll(toRemoveLayouts);
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
