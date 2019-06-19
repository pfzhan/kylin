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

package io.kyligence.kap.metadata.cube.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.validation.NIndexPlanValidator;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;

public class NIndexPlanManager implements IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NIndexPlanManager.class);

    public static NIndexPlanManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NIndexPlanManager.class);
    }

    // called by reflection
    static NIndexPlanManager newInstance(KylinConfig config, String project) {
        return new NIndexPlanManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<IndexPlan> crud;

    private NIndexPlanManager(KylinConfig cfg, final String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NIndexPlanManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(cfg), project);
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + IndexPlan.INDEX_PLAN_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<IndexPlan>(getStore(), resourceRootPath, IndexPlan.class) {
            @Override
            protected IndexPlan initEntityAfterReload(IndexPlan indexPlan, String resourceName) {
                indexPlan.initAfterReload(config, project);
                return indexPlan;
            }

            @Override
            protected IndexPlan initBrokenEntity(IndexPlan entity, String resourceName) {
                val indexPlan = super.initBrokenEntity(entity, resourceName);
                indexPlan.setProject(project);
                indexPlan.setConfig(KylinConfigExt.createInstance(config, Maps.newHashMap()));
                return indexPlan;
            }

        };
        this.crud.setCheckCopyOnWrite(true);
    }

    public IndexPlan copy(IndexPlan plan) {
        return crud.copyBySerialization(plan);
    }

    public IndexPlan getIndexPlan(String id) {
        return crud.get(id);
    }

    public IndexPlan getIndexPlanByModelAlias(String name) {
        return listAllIndexPlans(true).stream().filter(indexPlan -> Objects.equals(indexPlan.getModelAlias(), name))
                .findFirst().orElse(null);
    }

    // listAllIndexPlans only get the healthy indexPlans, the broken ones need to be invisible in the auto-suggestion process
    public List<IndexPlan> listAllIndexPlans() {
        return listAllIndexPlans(false);
    }

    // list all indexPlans include broken ones
    public List<IndexPlan> listAllIndexPlans(boolean includeBroken) {
        return Lists.newArrayList(
                crud.listAll().stream().filter(cp -> includeBroken || !cp.isBroken()).collect(Collectors.toList()));
    }

    public IndexPlan createIndexPlan(IndexPlan indexPlan) {
        if (indexPlan.getUuid() == null)
            throw new IllegalArgumentException();
        if (crud.contains(indexPlan.getUuid()))
            throw new IllegalArgumentException("IndexPlan '" + indexPlan.getUuid() + "' already exists");

        try {
            // init the cube plan if not yet
            if (indexPlan.getConfig() == null)
                indexPlan.initAfterReload(config, project);
        } catch (Exception e) {
            logger.warn("Broken cube plan " + indexPlan, e);
            indexPlan.addError(e.getMessage());
        }

        // Check base validation
        if (!indexPlan.getError().isEmpty()) {
            throw new IllegalArgumentException(indexPlan.getErrorMsg());
        }
        // Semantic validation
        NIndexPlanValidator validator = new NIndexPlanValidator();
        ValidateContext context = validator.validate(indexPlan);
        if (!context.ifPass()) {
            throw new IllegalArgumentException(indexPlan.getErrorMsg());
        }

        return save(indexPlan);
    }

    public interface NIndexPlanUpdater {
        void modify(IndexPlan copyForWrite);
    }

    public IndexPlan updateIndexPlan(String indexPlanId, NIndexPlanUpdater updater) {
        IndexPlan cached = getIndexPlan(indexPlanId);
        IndexPlan copy = copy(cached);
        updater.modify(copy);
        return updateIndexPlan(copy);
    }

    // use the NIndexPlanUpdater instead
    @Deprecated
    public IndexPlan updateIndexPlan(IndexPlan indexPlan) {
        if (indexPlan.isCachedAndShared())
            throw new IllegalStateException();

        if (indexPlan.getUuid() == null)
            throw new IllegalArgumentException();

        String name = indexPlan.getUuid();
        if (!crud.contains(name))
            throw new IllegalArgumentException("IndexPlan '" + name + "' does not exist.");

        try {
            // init the cube plan if not yet
            if (indexPlan.getConfig() == null)
                indexPlan.initAfterReload(config, project);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + indexPlan, e);
            indexPlan.addError(e.getMessage());
            throw new IllegalArgumentException(indexPlan.getErrorMsg());
        }

        return save(indexPlan);
    }

    // remove indexPlan
    public void dropIndexPlan(IndexPlan indexPlan) {
        crud.delete(indexPlan);
    }

    public void dropIndexPlan(String planId) {
        val indexPlan = getIndexPlan(planId);
        dropIndexPlan(indexPlan);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    private IndexPlan save(IndexPlan indexPlan) {
        validatePlan(indexPlan);
        indexPlan.setIndexes(indexPlan.getIndexes().stream()
                .peek(cuboid -> cuboid.setLayouts(cuboid.getLayouts().stream()
                        .filter(l -> l.isAuto() || l.getId() >= IndexEntity.TABLE_INDEX_START_ID)
                        .collect(Collectors.toList())))
                .filter(cuboid -> cuboid.getLayouts().size() > 0).collect(Collectors.toList()));

        val dataflowManager = NDataflowManager.getInstance(config, project);
        val dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        if (dataflow != null && dataflow.getLatestReadySegment() != null) {
            val livedIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
            val layoutIds = Sets.newHashSet(dataflow.getLatestReadySegment().getLayoutsMap().keySet());
            layoutIds.removeAll(livedIds);
            dataflowManager.removeLayouts(dataflow, layoutIds);
        }

        return crud.save(indexPlan);
    }

    private void validatePlan(IndexPlan indexPlan) {
        // make sure layout's measures and dimensions are equal to its index
        for (IndexEntity index : indexPlan.getIndexes()) {
            val layouts = index.getLayouts();
            for (LayoutEntity layout : layouts) {
                Preconditions.checkState(
                        CollectionUtils.isEqualCollection(layout.getColOrder().stream()
                                .filter(col -> col >= NDataModel.MEASURE_ID_BASE).collect(Collectors.toSet()),
                                index.getMeasures()),
                        "layout " + layout.getId() + "'s measure is illegal " + layout.getColOrder() + ", "
                                + index.getMeasures());
                Preconditions.checkState(CollectionUtils.isEqualCollection(layout.getColOrder().stream()
                        .filter(col -> col < NDataModel.MEASURE_ID_BASE).collect(Collectors.toSet()),
                        index.getDimensions()), "layout " + layout.getId() + "'s dimension is illegal");
            }
        }

        // make sure no layouts have same id
        val seen = Maps.<LayoutEntity, Long> newHashMap();
        val allDistinct = Stream
                .concat(indexPlan.getRuleBaseLayouts().stream(), indexPlan.getWhitelistLayouts().stream())
                .allMatch(layout -> {
                    if (seen.containsKey(layout)) {
                        return seen.get(layout) == layout.getId();
                    } else {
                        seen.put(layout, layout.getId());
                        return true;
                    }
                });
        Preconditions.checkState(allDistinct, "there are layouts have same id");

        // make sure cube_plan does not have duplicate indexes, duplicate index means two indexes have same dimensions and measures
        val allIndexes = indexPlan.getAllIndexes();
        val tableIndexSize = allIndexes.stream().filter(IndexEntity::isTableIndex).map(IndexEntity::getDimensionBitset)
                .distinct().count();
        val aggIndexSize = allIndexes.stream().filter(i -> !i.isTableIndex())
                .map(index -> index.getMeasureBitset().or(index.getDimensionBitset())).distinct().count();
        Preconditions.checkState(tableIndexSize + aggIndexSize == allIndexes.size(),
                "there are duplicate indexes in index_plan");
    }

}
