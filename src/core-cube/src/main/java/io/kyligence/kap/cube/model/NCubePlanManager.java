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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.cube.model.validation.NCubePlanValidator;
import io.kyligence.kap.metadata.project.NProjectManager;

public class NCubePlanManager implements IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NCubePlanManager.class);

    public static NCubePlanManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NCubePlanManager.class);
    }

    // called by reflection
    static NCubePlanManager newInstance(KylinConfig config, String project) {
        return new NCubePlanManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<NCubePlan> crud;

    private NCubePlanManager(KylinConfig cfg, final String project) {
        logger.info("Initializing NCubePlanManager with config " + config);
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + NCubePlan.CUBE_PLAN_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NCubePlan>(getStore(), resourceRootPath, NCubePlan.class) {
            @Override
            protected NCubePlan initEntityAfterReload(NCubePlan cubePlan, String resourceName) {
                try {
                    cubePlan.initAfterReload(config, project);
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
    }

    public NCubePlan findMatchingCubePlan(String modelName, String project, KylinConfig kylinConfig) {
        Set<IRealization> realizations = NProjectManager.getInstance(kylinConfig).listAllRealizations(project);
        for (IRealization realization : realizations) {
            if (realization instanceof NDataflow) {
                NCubePlan cubePlan = ((NDataflow) realization).getCubePlan();
                if (cubePlan.getModelName().equals(modelName)) {
                    return cubePlan;
                }
            }
        }
        throw new IllegalStateException("model " + modelName + " does not contain cube");
    }

    public NCubePlan copy(NCubePlan plan) {
        return crud.copyBySerialization(plan);
    }

    public NCubePlan getCubePlan(String name) {
        return crud.get(name);
    }

    public List<NCubePlan> listAllCubePlans() {
        return Lists.newArrayList(crud.listAll());
    }

    public NCubePlan createCubePlan(NCubePlan cubePlan) {
        if (cubePlan.getUuid() == null || cubePlan.getName() == null)
            throw new IllegalArgumentException();
        if (crud.contains(cubePlan.getName()))
            throw new IllegalArgumentException("NCubePlan '" + cubePlan.getName() + "' already exists");

        try {
            // init the cube plan if not yet
            if (cubePlan.getConfig() == null)
                cubePlan.initAfterReload(config, project);
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

        return saveCube(cubePlan);
    }

    public interface NCubePlanUpdater {
        void modify(NCubePlan copyForWrite);
    }

    public NCubePlan updateCubePlan(String cubePlanName, NCubePlanUpdater updater) {
        NCubePlan cached = getCubePlan(cubePlanName);
        NCubePlan copy = copy(cached);
        updater.modify(copy);
        return updateCubePlan(copy);
    }

    // use the NCubePlanUpdater instead
    @Deprecated
    public NCubePlan updateCubePlan(NCubePlan cubePlan) {
        if (cubePlan.isCachedAndShared())
            throw new IllegalStateException();

        if (cubePlan.getUuid() == null || cubePlan.getName() == null)
            throw new IllegalArgumentException();

        String name = cubePlan.getName();
        if (!crud.contains(name))
            throw new IllegalArgumentException("NCubePlan '" + name + "' does not exist.");

        try {
            // init the cube plan if not yet
            if (cubePlan.getConfig() == null)
                cubePlan.initAfterReload(config, project);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + cubePlan, e);
            cubePlan.addError(e.getMessage());
            throw new IllegalArgumentException(cubePlan.getErrorMsg());
        }

        return saveCube(cubePlan);
    }

    // remove cubePlan
    public void removeCubePlan(NCubePlan cubePlan) {
        crud.delete(cubePlan);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    private NCubePlan saveCube(NCubePlan cubePlan) {
        cubePlan.setCuboids(cubePlan.getCuboids().stream()
                .peek(cuboid -> cuboid.setLayouts(cuboid.getLayouts().stream()
                        .filter(l -> l.isAuto() || l.getId() >= NCuboidDesc.TABLE_INDEX_START_ID)
                        .collect(Collectors.toList())))
                .filter(cuboid -> cuboid.getLayouts().size() > 0).collect(Collectors.toList()));
        return crud.save(cubePlan);
    }
}
