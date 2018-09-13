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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dict.lookup.NSnapshotManager;
import org.apache.kylin.dict.lookup.NSnapshotTable;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;

public class NDataflowManager implements IRealizationProvider, IKeepNames {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowManager.class);

    public static NDataflowManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataflowManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataflowManager newInstance(KylinConfig config, String project) throws IOException {
        return new NDataflowManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    // NDataflow name ==> NDataflow
    private CaseInsensitiveStringCache<NDataflow> dataflowMap;
    private CachedCrudAssist<NDataflow> crud;

    // protects concurrent operations around the dataflowMap,
    // to avoid, for example, writing a dataflow in the middle of reloading it (dirty read)
    private AutoReadWriteLock dfMapLock = new AutoReadWriteLock();

    private NDataflowManager(KylinConfig cfg, final String project) throws IOException {
        logger.info("Initializing NDataflowManager with config " + cfg);
        this.config = cfg;
        this.project = project;
        this.dataflowMap = new CaseInsensitiveStringCache<>(config, project, "ncube");
        String resourceRootPath = "/" + project + NDataflow.DATAFLOW_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataflow>(getStore(), resourceRootPath, NDataflow.class, dataflowMap) {
            @Override
            protected NDataflow initEntityAfterReload(NDataflow df, String resourceName) {
                NCubePlan plan = NCubePlanManager.getInstance(config, project).getCubePlan(df.getCubePlanName());
                df.setProject(project);
                try {
                    df.initAfterReload((KylinConfigExt) plan.getConfig());
                } catch (Exception e) {
                    logger.warn("Broken NDataflow " + resourceName, e);
                    //TODO: vialate checkIsNotCachedAndShared
                    //df.setStatus(RealizationStatusEnum.DESCBROKEN);
                }
                return df;
            }
        };
        this.crud.setCheckCopyOnWrite(true);

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new NDataflowSyncListener(), project, "ncube");
    }

    private class NDataflowSyncListener extends Broadcaster.Listener {
        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : NProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real.getType().equals(getRealizationType())) {
                    try (AutoLock lock = dfMapLock.lockForWrite()) {
                        crud.reloadQuietly(real.getName());
                    }
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String dataflowName = cacheKey;

            try (AutoLock lock = dfMapLock.lockForWrite()) {
                if (event == Broadcaster.Event.DROP)
                    dataflowMap.removeLocal(dataflowName);
                else
                    crud.reloadQuietly(dataflowName);
            }

            broadcaster.notifyProjectDataUpdate(project);
        }
    }

    NDataflow reloadDataFlow(String dataFlowName) {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            return crud.reload(dataFlowName);
        }
    }

    public LookupStringTable getLookupTable(NDataSegment cubeSegment, JoinDesc join) {
        long ts = System.currentTimeMillis();

        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(cubeSegment.getConfig(),
                cubeSegment.getProject());
        NSnapshotManager snapshotMgr = NSnapshotManager.getInstance(cubeSegment.getConfig(), cubeSegment.getProject());

        String tableName = join.getPKSide().getTableIdentity();
        String[] pkCols = join.getPrimaryKey();
        String snapshotResPath = cubeSegment.getSnapshots().get(tableName);
        if (snapshotResPath == null)
            throw new IllegalStateException(
                    "No snaphot for table '" + tableName + "' found on cube segment " + cubeSegment.getName());

        try {
            NSnapshotTable snapshot = snapshotMgr.getSnapshotTable(snapshotResPath);
            TableDesc tableDesc = metaMgr.getTableDesc(tableName);
            LookupStringTable enhancedStringLookupTable = new LookupStringTable(tableDesc, pkCols, snapshot);
            logger.info("Time to get lookup up table for {} is {} ", join.getPKSide().getTableName(),
                    (System.currentTimeMillis() - ts));
            return enhancedStringLookupTable;
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load lookup table " + tableName + " from snapshot " + snapshotResPath, e);
        }
    }

    @Override
    public String getRealizationType() {
        return NDataflow.REALIZATION_TYPE;
    }

    @Override
    public IRealization getRealization(String name) {
        return getDataflow(name);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public List<NDataflow> listAllDataflows() {
        try (AutoLock lock = dfMapLock.lockForRead()) {
            return new ArrayList<>(dataflowMap.values());
        }
    }

    public NDataflow getDataflow(String name) {
        try (AutoLock lock = dfMapLock.lockForRead()) {
            return dataflowMap.get(name);
        }
    }

    public NDataflow getDataflowByUuid(String uuid) {
        try (AutoLock lock = dfMapLock.lockForRead()) {
            Collection<NDataflow> copy = new ArrayList<>(dataflowMap.values());
            for (NDataflow df : copy) {
                if (uuid.equals(df.getUuid()))
                    return df;
            }
            return null;
        }
    }

    public List<NDataflow> getDataflowsByCubePlan(String cubePlan) {
        List<NDataflow> list = listAllDataflows();
        List<NDataflow> result = new ArrayList<NDataflow>();
        Iterator<NDataflow> it = list.iterator();
        while (it.hasNext()) {
            NDataflow df = it.next();
            if (cubePlan.equals(df.getCubePlanName())) {
                result.add(df);
            }
        }
        return result;
    }

    public NDataflow createDataflow(String dfName, String projectName, NCubePlan plan, String owner)
            throws IOException {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            NDataflow df = NDataflow.create(dfName, plan);
            return createDataflow(df, projectName, owner);
        }
    }

    public NDataflow createDataflow(NDataflow df, String projectName, String owner) throws IOException {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            logger.info("Creating NDataflow '" + projectName + "-->" + df.getName() + "' from instance object.");

            // save dataflow
            df.setOwner(owner);
            df.getSegments().validate();
            crud.save(df);

            // add to project
            NProjectManager.getInstance(config).moveRealizationToProject(getRealizationType(), df.getName(),
                    projectName, owner);

            return df;
        }
    }

    public NDataSegment appendSegment(NDataflow df, SegmentRange segRange) throws IOException {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            checkBuildingSegment(df);

            //            // case of full build
            //            if (!df.getModel().getPartitionDesc().isPartitioned()) {
            //                segRange = null;
            //            }

            NDataSegment newSegment = newSegment(df, segRange);
            validateNewSegments(df, newSegment);

            NDataflowUpdate upd = new NDataflowUpdate(df.getName());
            upd.setToAddSegs(newSegment);
            updateDataflow(upd);
            return newSegment;
        }
    }

    public NDataSegment refreshSegment(NDataflow df, SegmentRange segRange) throws IOException {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            checkBuildingSegment(df);

            NDataSegment newSegment = newSegment(df, segRange);

            Pair<Boolean, Boolean> pair = df.getSegments().fitInSegments(newSegment);
            if (!pair.getFirst() || !pair.getSecond())
                throw new IllegalArgumentException("The new refreshing segment " + newSegment
                        + " does not match any existing segment in NDataflow " + df);

            if (segRange != null) {
                NDataSegment toRefreshSeg = null;
                for (NDataSegment NDataSegment : df.getSegments()) {
                    if (NDataSegment.getSegRange().equals(segRange)) {
                        toRefreshSeg = NDataSegment;
                        break;
                    }
                }

                if (toRefreshSeg == null) {
                    throw new IllegalArgumentException(
                            "For streaming NDataflow, only one segment can be refreshed at one time");
                }

                newSegment.setSegmentRange(toRefreshSeg.getSegRange());
            }

            NDataflowUpdate upd = new NDataflowUpdate(df.getName());
            upd.setToAddSegs(newSegment);
            updateDataflow(upd);

            return newSegment;
        }
    }

    public NDataSegment mergeSegments(NDataflow dataflow, SegmentRange segRange, boolean force) throws IOException {
        NDataflow dataflowCopy = dataflow.copy();
        if (dataflowCopy.getSegments().isEmpty())
            throw new IllegalArgumentException(dataflow + " has no segments");
        Preconditions.checkArgument(segRange != null);

        checkBuildingSegment(dataflowCopy);
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
            if (!firstSegDetails.checkCuboidsBeforeMerge(details))
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
                if (seg.getSegDetails().getTotalCuboidRowCount() == 0) {
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
        newSegment.setTimeRange(new TimeRange(first.getTSRange().getStart(), last.getTSRange().getEnd()));
        validateNewSegments(dataflowCopy, newSegment);

        NDataflowUpdate update = new NDataflowUpdate(dataflowCopy.getName());
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

    private void checkBuildingSegment(NDataflow df) {
        int maxBuldingSeg = df.getConfig().getMaxBuildingSegments();
        if (df.getBuildingSegments().size() >= maxBuldingSeg) {
            throw new IllegalStateException(
                    "There is already " + df.getBuildingSegments().size() + " building segment; ");
        }
    }

    private NDataSegment newSegment(NDataflow df, SegmentRange segRange) {
        // BREAKING CHANGE: remove legacy caring as in org.apache.kylin.cube.CubeManager.SegmentAssist.newSegment()
        Preconditions.checkNotNull(segRange);

        NDataSegment lastSeg = df.getSegmentWithMaxId();
        NDataSegment segment = new NDataSegment();
        segment.setId(lastSeg == null ? 0 : lastSeg.getId() + 1);
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
            throw new IllegalStateException("For NDataflow " + df.getName() + ", the new segments " + newList
                    + " do not fit in its current " + df.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    public NDataflow copy(NDataflow df) {
        return crud.copyBySerialization(df);
    }

    public interface NDataflowUpdater {
        void modify(NDataflow copyForWrite);
    }

    public NDataflow updateDataflow(String dfName, NDataflowUpdater updater) throws IOException {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            return updateDataflowWithRetry(dfName, updater, 3);
        }
    }

    public long getSegmentSize(NDataSegment segment) {
        long size = 0L;
        Collection<NDataCuboid> nDataCuboids = segment.getCuboidsMap().values();
        for (NDataCuboid nDataCuboid : nDataCuboids) {
            size += nDataCuboid.getSizeKB();
        }
        return size;
    }

    private NDataflow updateDataflowWithRetry(String dfName, NDataflowUpdater updater, int retry) throws IOException {
        RuntimeException firstException = null;

        for (; retry >= 0; retry--) {
            NDataflow cached = getDataflow(dfName);
            NDataflow copy = copy(cached);
            updater.modify(copy);
            try {
                NDataflow df = crud.save(copy);

                //this is a duplicate call to take care of scenarios where REST cache service unavailable
                NProjectManager.getInstance(df.getConfig()).clearL2Cache();

                return df;
            } catch (IllegalStateException ex) {
                if (firstException == null)
                    firstException = ex;
                logger.error("Write conflict during update NDataflow " + dfName + ", retries left: " + retry, ex);
            }
        }
        throw firstException;
    }

    public NDataflow updateDataflow(final NDataflowUpdate update) throws IOException {
        return updateDataflow(update.getDataflowName(), new NDataflowUpdater() {
            @Override
            public void modify(NDataflow copyForWrite) {
                NDataflow df = copyForWrite;
                Segments<NDataSegment> newSegs = (Segments<NDataSegment>) df.getSegments().clone();

                if (update.getToAddSegs() != null) {
                    for (NDataSegment seg : update.getToAddSegs()) {
                        seg.setDataflow(df);
                        newSegs.add(seg);
                    }
                }

                if (update.getToUpdateSegs() != null) {
                    for (NDataSegment seg : update.getToUpdateSegs()) {
                        seg.setDataflow(df);
                        for (int i = 0; i < newSegs.size(); i++) {
                            if (newSegs.get(i).getId() == seg.getId()) {
                                newSegs.set(i, seg);
                                break;
                            }
                        }
                    }
                }

                if (update.getToRemoveSegs() != null) {
                    Iterator<NDataSegment> iterator = newSegs.iterator();
                    while (iterator.hasNext()) {
                        NDataSegment currentSeg = iterator.next();
                        for (NDataSegment toRemoveSeg : update.getToRemoveSegs()) {
                            if (currentSeg.getId() == toRemoveSeg.getId()) {
                                logger.info("Remove segment " + currentSeg.toString());
                                iterator.remove();
                                break;
                            }
                        }
                    }
                }

                Collections.sort(newSegs);
                newSegs.validate();
                df.setSegments(newSegs);

                if (update.getStatus() != null) {
                    df.setStatus(update.getStatus());
                }

                if (update.getDescription() != null) {
                    df.setDescription(update.getDescription());
                }

                if (update.getOwner() != null) {
                    df.setOwner(update.getOwner());
                }

                if (update.getCost() > 0) {
                    df.setCost(update.getCost());
                }

                // NDataCuboid updates are idempotent, safe to re-do (retry) many times
                try {
                    NDataSegDetailsManager.getInstance(df.getConfig(), project).updateDataflow(df, update);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public NDataflow dropDataflow(String dfName) throws IOException {
        try (AutoLock lock = dfMapLock.lockForWrite()) {
            logger.info("Dropping NDataflow '" + dfName + "'");

            NDataflow df = getDataflow(dfName);

            // delete NDataSegDetails first
            NDataSegDetailsManager segDetailsManager = NDataSegDetailsManager.getInstance(config, project);
            for (NDataSegment seg : df.getSegments()) {
                segDetailsManager.removeForSegment(df, seg.getId());
            }

            // remove NDataflow and update cache
            crud.delete(df);

            // delete NDataflow from project
            NProjectManager.getInstance(config).removeRealizationsFromProject(project, getRealizationType(), dfName);

            return df;
        }
    }

    public List<NDataSegment> calculateHoles(String dfName) {
        List<NDataSegment> holes = Lists.newArrayList();
        final NDataflow df = getDataflow(dfName);
        Preconditions.checkNotNull(df);
        final List<NDataSegment> segments = df.getSegments();
        if (segments.size() == 0) {
            return holes;
        }

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
}
