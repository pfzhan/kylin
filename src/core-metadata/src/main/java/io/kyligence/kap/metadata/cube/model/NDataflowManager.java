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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.var;

public class NDataflowManager implements IRealizationProvider, IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowManager.class);

    public static NDataflowManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataflowManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataflowManager newInstance(KylinConfig config, String project) {
        return new NDataflowManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<NDataflow> crud;

    private NDataflowManager(KylinConfig cfg, final String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NDataflowManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(cfg), project);
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + NDataflow.DATAFLOW_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataflow>(getStore(), resourceRootPath, NDataflow.class) {
            @Override
            protected NDataflow initEntityAfterReload(NDataflow df, String resourceName) {
                IndexPlan plan = NIndexPlanManager.getInstance(config, project).getIndexPlan(df.getUuid());
                df.initAfterReload((KylinConfigExt) plan.getConfig(), project);
                return df;
            }

            @Override
            protected NDataflow initBrokenEntity(NDataflow entity, String resourceName) {
                val dataflow = super.initBrokenEntity(entity, resourceName);
                IndexPlan plan = NIndexPlanManager.getInstance(config, project).getIndexPlan(resourceName);
                dataflow.setConfig((KylinConfigExt) plan.getConfig());
                dataflow.setProject(project);
                dataflow.setDependencies(dataflow.calcDependencies());
                return dataflow;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    public NDataflow removeLayouts(NDataflow df, Collection<Long> tobeRemoveCuboidLayoutIds) {
        List<NDataLayout> tobeRemoveCuboidLayout = Lists.newArrayList();
        Segments<NDataSegment> segments = df.getSegments();
        for (NDataSegment segment : segments) {
            for (Long tobeRemoveCuboidLayoutId : tobeRemoveCuboidLayoutIds) {
                NDataLayout dataCuboid = segment.getLayout(tobeRemoveCuboidLayoutId);
                if (dataCuboid == null) {
                    continue;
                }
                tobeRemoveCuboidLayout.add(dataCuboid);
            }
        }

        if (CollectionUtils.isNotEmpty(tobeRemoveCuboidLayout)) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveLayouts(tobeRemoveCuboidLayout.toArray(new NDataLayout[0]));
            return updateDataflow(update);
        }
        return df;
    }

    @Override
    public String getRealizationType() {
        return NDataflow.REALIZATION_TYPE;
    }

    @Override
    public IRealization getRealization(String id) {
        val df = getDataflow(id);
        if (df == null || df.checkBrokenWithRelatedInfo()) {
            return null;
        }
        return df;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    // listAllDataflows only get the healthy dataflows,
    // the broken ones need to be invisible in the auto-suggestion process,
    // anyone in dataflow, indexPlan and dataModel is broken, the dataflow is considered to be broken
    public List<NDataflow> listAllDataflows() {
        return listAllDataflows(false);
    }

    // get all dataflows include broken ones
    public List<NDataflow> listAllDataflows(boolean includeBroken) {
        return crud.listAll().stream().filter(df -> includeBroken || !df.checkBrokenWithRelatedInfo())
                .collect(Collectors.toList());
    }

    // listUnderliningDataModels only get the healthy models,
    // the broken ones need to be invisible in the auto-suggestion process,
    // anyone in dataflow, indexPlan and dataModel is broken, the model is considered to be broken
    public List<NDataModel> listUnderliningDataModels() {
        return listUnderliningDataModels(false);
    }

    public List<NDataModel> listDataModelsByStatus(RealizationStatusEnum status) {
        List<NDataflow> dataflows = listAllDataflows();
        List<NDataModel> onlineModels = Lists.newArrayList();
        for (NDataflow dataflow : dataflows) {
            if (status == dataflow.getStatus()) {
                onlineModels.add(dataflow.getModel());
            }
        }
        return onlineModels;
    }

    // get all models include broken ones
    public List<NDataModel> listUnderliningDataModels(boolean includeBroken) {
        val dataflows = listAllDataflows(includeBroken);
        return dataflows.stream().map(NDataflow::getModel).collect(Collectors.toList());
    }

    // within a project, find models that use the specified table
    public List<NDataModel> getModelsUsingTable(TableDesc table) {
        List<NDataModel> models = new ArrayList<>();
        for (NDataModel modelDesc : listUnderliningDataModels()) {
            if (modelDesc.containsTable(table))
                models.add(modelDesc);
        }
        return models;
    }

    // within a project, find models that use the specified table as root table
    public List<NDataModel> getModelsUsingRootTable(TableDesc table) {
        List<NDataModel> models = new ArrayList<>();
        for (NDataModel modelDesc : listUnderliningDataModels()) {
            if (modelDesc.isRootFactTable(table)) {
                models.add(modelDesc);
            }
        }
        return models;
    }

    public List<NDataModel> getTableOrientedModelsUsingRootTable(TableDesc table) {
        List<NDataModel> models = new ArrayList<>();
        for (NDataModel modelDesc : listUnderliningDataModels()) {
            if (modelDesc.isRootFactTable(table)
                    && modelDesc.getManagementType().equals(ManagementType.TABLE_ORIENTED)) {
                models.add(modelDesc);
            }
        }
        return models;
    }

    public NDataflow getDataflow(String id) {
        if (StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    public NDataflow getDataflowByModelAlias(String name) {
        return listAllDataflows(true).stream().filter(dataflow -> Objects.equals(dataflow.getModelAlias(), name))
                .findFirst().orElse(null);
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    public NDataflow createDataflow(IndexPlan plan, String owner) {
        NDataflow df = NDataflow.create(plan);
        df.initAfterReload((KylinConfigExt) plan.getConfig(), project);

        // save dataflow
        df.setOwner(owner);
        df.getSegments().validate();
        crud.save(df);

        fillDf(df);

        return df;
    }

    public void fillDf(NDataflow df) {
        // if it's table oriented, create segments at once
        if (df.getModel().getManagementType() != ManagementType.TABLE_ORIENTED) {
            return;
        }
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        String tableName = df.getModel().getRootFactTable().getTableIdentity();
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(tableName);
        val segmentRanges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(dataLoadingRange);
        if (CollectionUtils.isNotEmpty(segmentRanges)) {
            fillDfWithNewRanges(df, segmentRanges);
        }

    }

    public void fillDfWithNewRanges(NDataflow df, List<SegmentRange> segmentRanges) {
        Segments<NDataSegment> segs = new Segments<>();

        segmentRanges.forEach(segRange -> {
            NDataSegment newSegment = newSegment(df, segRange);
            newSegment.setStatus(SegmentStatusEnum.READY);
            segs.add(newSegment);
        });
        val update = new NDataflowUpdate(df.getUuid());
        update.setToAddSegs(segs.toArray(new NDataSegment[0]));
        updateDataflow(update);
    }

    public NDataSegment appendSegment(NDataflow df, SegmentRange segRange) {

        NDataSegment newSegment = newSegment(df, segRange);
        validateNewSegments(df, newSegment);

        NDataflowUpdate upd = new NDataflowUpdate(df.getUuid());
        upd.setToAddSegs(newSegment);
        updateDataflow(upd);
        return newSegment;
    }

    public NDataSegment appendSegmentForStreaming(NDataflow df, SegmentRange segRange) {
        NDataSegment newSegment = newSegment(df, segRange);
        newSegment.setStatus(SegmentStatusEnum.NEW);

        Map<Long, NDataLayout> layoutsMap = new HashMap<>();
        for (LayoutEntity layout : df.getIndexPlan().getAllLayouts()) {
            NDataLayout ly = NDataLayout.newDataLayout(df, newSegment.getId(), layout.getId());
            layoutsMap.put(ly.getLayoutId(), ly);
        }
        newSegment.setLayoutsMap(layoutsMap);
        validateNewSegments(df, newSegment);
        NDataflowUpdate upd = new NDataflowUpdate(df.getUuid());
        upd.setToAddSegs(newSegment);
        updateDataflow(upd);
        return newSegment;
    }

    public NDataSegment refreshSegment(NDataflow df, SegmentRange segRange) {

        NDataSegment newSegment = newSegment(df, segRange);

        NDataSegment toRefreshSeg = null;
        for (NDataSegment NDataSegment : df.getSegments()) {
            if (NDataSegment.getSegRange().equals(segRange)) {
                toRefreshSeg = NDataSegment;
                break;
            }
        }

        if (toRefreshSeg == null) {
            throw new IllegalArgumentException(String.format("no ready segment with range %s exists on model %s",
                    segRange.toString(), df.getModelAlias()));
        }

        newSegment.setSegmentRange(toRefreshSeg.getSegRange());

        NDataflowUpdate upd = new NDataflowUpdate(df.getUuid());
        upd.setToAddSegs(newSegment);
        updateDataflow(upd);

        return newSegment;
    }

    public NDataSegment mergeSegments(NDataflow dataflow, SegmentRange segRange, boolean force) {
        NDataflow dataflowCopy = dataflow.copy();
        if (dataflowCopy.getSegments().isEmpty())
            throw new IllegalArgumentException(dataflow + " has no segments");
        Preconditions.checkArgument(segRange != null);

        checkCubeIsPartitioned(dataflowCopy);

        NDataSegment newSegment = newSegment(dataflowCopy, segRange);
        Segments<NDataSegment> mergingSegments = dataflowCopy.getMergingSegments(newSegment);
        if (mergingSegments.size() <= 1)
            throw new IllegalArgumentException("Range " + newSegment.getSegRange()
                    + " must contain at least 2 segments, but there is " + mergingSegments.size());

        NDataSegment first = mergingSegments.get(0);
        NDataSegDetails firstSegDetails = first.getSegDetails();
        for (int i = 1; i < mergingSegments.size(); i++) {
            NDataSegment dataSegment = mergingSegments.get(i);
            NDataSegDetails details = dataSegment.getSegDetails();
            if (!firstSegDetails.checkLayoutsBeforeMerge(details))
                throw new IllegalArgumentException(first + " and " + dataSegment + " has different layout status");
        }

        if (!force) {
            for (int i = 0; i < mergingSegments.size() - 1; i++) {
                if (!mergingSegments.get(i).getSegRange().connects(mergingSegments.get(i + 1).getSegRange()))
                    throw new IllegalStateException("Merging segments must not have gaps between "
                            + mergingSegments.get(i) + " and " + mergingSegments.get(i + 1));
            }

            List<String> emptySegment = Lists.newArrayList();
            for (NDataSegment seg : mergingSegments) {
                if (seg.getSegDetails().getTotalRowCount() == 0) {
                    emptySegment.add(seg.getName());
                }
            }
            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException(
                        "Empty cube segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: "
                                + emptySegment);
            }
        }

        NDataSegment last = mergingSegments.get(mergingSegments.size() - 1);
        newSegment.setSegmentRange(first.getSegRange().coverWith(last.getSegRange()));

        if (first.isOffsetCube()) {
            newSegment.setSegmentRange(segRange);
        } else {
            newSegment.setTimeRange(new TimeRange(first.getTSRange().getStart(), last.getTSRange().getEnd()));
        }

        validateNewSegments(dataflowCopy, newSegment);

        NDataflowUpdate update = new NDataflowUpdate(dataflowCopy.getUuid());
        update.setToAddSegs(newSegment);
        updateDataflow(update);
        return newSegment;
    }

    private void checkCubeIsPartitioned(NDataflow dataflow) {
        if (!dataflow.getModel().getPartitionDesc().isPartitioned()) {
            throw new IllegalStateException(
                    "there is no partition date column specified, only full build is supported");
        }
    }

    @VisibleForTesting
    NDataSegment newSegment(NDataflow df, SegmentRange segRange) {
        // BREAKING CHANGE: remove legacy caring as in org.apache.kylin.cube.CubeManager.SegmentAssist.newSegment()
        Preconditions.checkNotNull(segRange);

        NDataSegment segment = new NDataSegment();
        segment.setId(UUID.randomUUID().toString());
        segment.setName(Segments.makeSegmentName(segRange));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDataflow(df);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.setSegmentRange(segRange);
        segment.validate();
        return segment;
    }

    private void validateNewSegments(NDataflow df, NDataSegment newSegments) {
        List<NDataSegment> tobe = df.calculateToBeSegments(newSegments);
        List<NDataSegment> newList = Arrays.asList(newSegments);
        if (!tobe.containsAll(newList)) {
            throw new IllegalStateException("For NDataflow " + df + ", the new segments " + newList
                    + " do not fit in its current " + df.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    public List<NDataSegment> getToRemoveSegs(NDataflow dataflow, NDataSegment segment) {
        Segments tobe = dataflow.calculateToBeSegments(segment);

        if (!tobe.contains(segment))
            throw new IllegalStateException(
                    "For NDataflow " + dataflow + ", segment " + segment + " is expected but not in the tobe " + tobe);

        if (segment.getStatus() == SegmentStatusEnum.NEW)
            segment.setStatus(SegmentStatusEnum.READY);

        List<NDataSegment> toRemoveSegs = Lists.newArrayList();
        for (NDataSegment s : dataflow.getSegments()) {
            if (!tobe.contains(s))
                toRemoveSegs.add(s);
        }

        logger.info("promoting new ready segment {} in dataflow {}, segments to removed: {}", segment, dataflow,
                toRemoveSegs);

        return toRemoveSegs;
    }

    public NDataflow copy(NDataflow df) {
        return crud.copyBySerialization(df);
    }

    public List<NDataflow> getDataflowsByTableAndStatus(String tableName, RealizationStatusEnum status) {
        val tableManager = NTableMetadataManager.getInstance(config, project);
        val table = tableManager.getTableDesc(tableName);
        val models = getTableOrientedModelsUsingRootTable(table);
        List<NDataflow> dataflows = Lists.newArrayList();
        for (val model : models) {
            dataflows.add(getDataflow(model.getUuid()));
        }
        return dataflows.stream().filter(dataflow -> dataflow.getStatus().equals(status)).collect(Collectors.toList());

    }

    public void fillDfManually(NDataflow df, List<SegmentRange> ranges) {
        Preconditions.checkState(df.getModel().getManagementType() == ManagementType.MODEL_BASED);
        if (CollectionUtils.isEmpty(ranges)) {
            return;
        }
        fillDfWithNewRanges(df, ranges);
    }

    public NDataflow handleRetention(NDataflow df) {
        Segments<NDataSegment> segsToRemove = df.getSegmentsToRemoveByRetention();
        if (CollectionUtils.isEmpty(segsToRemove)) {
            return df;
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(segsToRemove.toArray(new NDataSegment[segsToRemove.size()]));
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, project);
        val model = df.getModel();
        loadingRangeManager.updateCoveredRangeAfterRetention(model, segsToRemove.getLastSegment());
        return updateDataflow(update);
    }

    public interface NDataflowUpdater {
        void modify(NDataflow copyForWrite);
    }

    /**
     * update the dataflow from the restore by lambda function updater.
     * sometimes, dataflow's segments is removed, but do not from the restore, need to remove again.
     *
     * @param dfId
     * @param updater
     * @return
     */
    public NDataflow updateDataflow(String dfId, NDataflowUpdater updater) {
        NDataflow cached = getDataflow(dfId);
        NDataflow copy = copy(cached);
        updater.modify(copy);
        if (copy.getSegments().stream().map(seg -> seg.getLayoutsMap().keySet()).distinct().count() > 1) {
            logger.warn("Dataflow <{}> is not a prefect square", dfId);
        }

        Set<String> copySegIdSet = copy.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
        val nDataSegDetailsManager = NDataSegDetailsManager.getInstance(cached.getConfig(), project);
        for (NDataSegment segment : cached.getSegments()) {
            if (!copySegIdSet.contains(segment.getId())) {
                nDataSegDetailsManager.removeForSegment(copy, segment.getId());
            }
        }

        return crud.save(copy);
    }

    public long getDataflowStorageSize(String modelId) {
        return getDataflow(modelId).getStorageBytesSize();
    }

    public long getDataflowSourceSize(String modelId) {
        return getDataflow(modelId).getSourceBytesSize();
    }

    public NDataflow updateDataflow(final NDataflowUpdate update) {
        return updateDataflow(update.getDataflowId(), copyForWrite -> {
            NDataflow df = copyForWrite;
            Segments<NDataSegment> newSegs = (Segments<NDataSegment>) df.getSegments().clone();

            Arrays.stream(Optional.ofNullable(update.getToAddSegs()).orElse(new NDataSegment[0])).forEach(seg -> {
                seg.setDataflow(df);
                newSegs.add(seg);
            });

            Arrays.stream(Optional.ofNullable(update.getToUpdateSegs()).orElse(new NDataSegment[0])).forEach(seg -> {
                seg.setDataflow(df);
                newSegs.replace(Comparator.comparing(NDataSegment::getId), seg);
            });

            if (update.getToRemoveSegs() != null) {
                Iterator<NDataSegment> iterator = newSegs.iterator();
                val toRemoveIds = Arrays.stream(update.getToRemoveSegs()).map(NDataSegment::getId)
                        .collect(Collectors.toSet());
                while (iterator.hasNext()) {
                    NDataSegment currentSeg = iterator.next();
                    if (toRemoveIds.contains(currentSeg.getId())) {
                        logger.info("Remove segment {}", currentSeg);
                        iterator.remove();
                    }
                }
            }

            Arrays.stream(Optional.ofNullable(update.getToRemoveLayouts()).orElse(new NDataLayout[0]))
                    .forEach(removeLayout -> df.getLayoutHitCount().remove(removeLayout.getLayoutId()));

            df.setSegments(newSegs);

            val newStatus = Optional.ofNullable(update.getStatus()).orElse(df.getStatus());
            df.setStatus(newStatus);

            val newDesc = Optional.ofNullable(update.getDescription()).orElse(df.getDescription());
            df.setDescription(newDesc);

            val newOwner = Optional.ofNullable(update.getOwner()).orElse(df.getOwner());
            df.setOwner(newOwner);

            df.setCost(update.getCost() > 0 ? update.getCost() : df.getCost());

            NDataSegDetailsManager.getInstance(df.getConfig(), project).updateDataflow(df, update);
        });
    }

    public NDataflow dropDataflow(String dfId) {
        NDataflow df = getDataflow(dfId);
        var dfInfo = dfId;
        if (df != null) {
            dfInfo = df.toString();
        } else {
            logger.warn("Dropping NDataflow '{}' does not exist", dfInfo);
            return null;
        }
        logger.info("Dropping NDataflow '{}'", dfInfo);

        // delete NDataSegDetails first
        NDataSegDetailsManager segDetailsManager = NDataSegDetailsManager.getInstance(config, project);
        segDetailsManager.removeDetails(df);

        // remove NDataflow and update cache
        crud.delete(df);

        return df;
    }

    List<NDataSegment> calculateHoles(String dfId) {
        final NDataflow df = getDataflow(dfId);
        Preconditions.checkNotNull(df);
        return calculateHoles(dfId, df.getSegments());
    }

    public List<NDataSegment> calculateHoles(String dfId, List<NDataSegment> segments) {
        List<NDataSegment> holes = Lists.newArrayList();
        final NDataflow df = getDataflow(dfId);
        Preconditions.checkNotNull(df);

        Collections.sort(segments);
        for (int i = 0; i < segments.size() - 1; ++i) {
            NDataSegment first = segments.get(i);
            NDataSegment second = segments.get(i + 1);
            if (first.getSegRange().connects(second.getSegRange()))
                continue;

            if (first.getSegRange().apartBefore(second.getSegRange())) {
                NDataSegment hole = new NDataSegment();
                hole.setDataflow(df);

                // TODO: fix segment
                hole.setSegmentRange(first.getSegRange().gapTill(second.getSegRange()));
                hole.setTimeRange(new TimeRange(first.getTSRange().getEnd(), second.getTSRange().getStart()));
                hole.setName(Segments.makeSegmentName(hole.getSegRange()));
                holes.add(hole);
            }
        }
        return holes;
    }

    public List<SegmentRange> calculateSegHoles(String dfId) {
        return calculateHoles(dfId).stream().map(NDataSegment::getSegRange).collect(Collectors.toList());
    }

    public List<NDataSegment> checkHoleIfNewSegBuild(String dfId, SegmentRange toBuildSegment) {
        final NDataflow df = getDataflow(dfId);
        List<NDataSegment> segments = Lists.newArrayList(df.getSegments());
        if (toBuildSegment != null) {
            NDataSegment toBuildSeg = new NDataSegment();
            toBuildSeg.setDataflow(df);
            toBuildSeg.setSegmentRange(toBuildSegment);
            toBuildSeg.setName(Segments.makeSegmentName(toBuildSegment));
            segments.add(toBuildSeg);
        }

        return calculateHoles(dfId, segments);
    }

}
