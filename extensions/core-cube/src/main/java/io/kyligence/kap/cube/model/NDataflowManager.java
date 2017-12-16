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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceNotFoundException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;

public class NDataflowManager implements IRealizationProvider, IKeepNames {
    public static final Serializer<NDataflow> DATAFLOW_SERIALIZER = new JsonSerializer<>(NDataflow.class);

    private static final Logger logger = LoggerFactory.getLogger(NDataflowManager.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, NDataflowManager> CACHE = new ConcurrentHashMap<KylinConfig, NDataflowManager>();

    public static NDataflowManager getInstance(KylinConfig config) {
        NDataflowManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (NDataflowManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new NDataflowManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                    for (KylinConfig kylinConfig : CACHE.keySet()) {
                        logger.warn("type: " + kylinConfig.getClass() + " reference: "
                                + System.identityHashCode(kylinConfig.base()));
                    }
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init NDataflowManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;

    // NDataflow name ==> NDataflow
    private CaseInsensitiveStringCache<NDataflow> dataflowMap;

    // protects concurrent operations around the dataflowMap, 
    // to avoid, for example, writing a dataflow in the middle of reloading it (dirty read)
    private ReadWriteLock dfMapLock = new ReentrantReadWriteLock();

    private NDataflowManager(KylinConfig config) throws IOException {
        logger.info("Initializing NDataflowManager with config " + config);
        this.config = config;
        this.dataflowMap = new CaseInsensitiveStringCache<>(config, "ncube");

        // touch lower level metadata before registering my listener
        loadAllDataflows();
        Broadcaster.getInstance(config).registerListener(new NDataflowSyncListener(), "ncube");
    }

    public LookupStringTable getLookupTable(NDataSegment lastSeg, JoinDesc join) {
        throw new UnsupportedOperationException("derived not support yet.");
    }

    private class NDataflowSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real.getType().equals(getRealizationType())) {
                    reloadDataflowLocalQuietly(real.getName());
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String dataflowName = cacheKey;

            if (event == Broadcaster.Event.DROP)
                removeDataflowLocal(dataflowName);
            else
                reloadDataflowLocalQuietly(dataflowName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(getRealizationType(),
                    dataflowName)) {
                broadcaster.notifyProjectDataUpdate(prj.getName());
            }
        }

        private void reloadDataflowLocalQuietly(String name) {
            try {
                reloadDataflowLocal(name);
            } catch (ResourceNotFoundException ex) { // ResourceNotFoundException is normal a resource is deleted
                logger.warn("Failed to load dataflow at " + name + ", brief: " + ex.toString());
            }
        }
    }

    private void loadAllDataflows() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(NDataflow.DATAFLOW_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);

        logger.info("Loading NDataflow from folder " + store.getReadableResourcePath(NDataflow.DATAFLOW_RESOURCE_ROOT));

        int succeed = 0;
        int fail = 0;
        for (String path : paths) {
            NDataflow df = reloadDataflowLocalAt(path);
            if (df == null) {
                fail++;
            } else {
                succeed++;
            }
        }

        logger.info("Loaded " + succeed + " NDataflows, fail on " + fail + " NDataflows");
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

    // append a full build segment
    public NDataSegment appendSegment(NDataflow df) throws IOException {
        return appendSegment(df, null, null, null);
    }

    public NDataSegment appendSegment(NDataflow df, SegmentRange segRange) throws IOException {
        return appendSegment(df, segRange, null, null);
    }

    public NDataSegment appendSegment(NDataflow df, SourcePartition src) throws IOException {
        Preconditions.checkArgument(src.getTSRange() == null);
        return appendSegment(df, src.getSegRange(), src.getSourcePartitionOffsetStart(),
                src.getSourcePartitionOffsetEnd());
    }

    NDataSegment appendSegment(NDataflow df, SegmentRange segRange, Map<Integer, Long> sourcePartitionOffsetStart,
            Map<Integer, Long> sourcePartitionOffsetEnd) throws IOException {
        dfMapLock.writeLock().lock();
        try {
            checkBuildingSegment(df);

            // case of full build
            if (!df.getModel().getPartitionDesc().isPartitioned()) {
                segRange = null;
            }

            NDataSegment newSegment = newSegment(df, segRange);
            newSegment.setSourcePartitionOffsetStart(sourcePartitionOffsetStart);
            newSegment.setSourcePartitionOffsetEnd(sourcePartitionOffsetEnd);
            validateNewSegments(df, newSegment);

            NDataflowUpdate upd = new NDataflowUpdate(df);
            upd.setToAddSegs(newSegment);
            updateDataflow(upd);
            return newSegment;
        } finally {
            dfMapLock.writeLock().unlock();
        }
    }

    public NDataSegment refreshSegment(NDataflow df, SegmentRange segRange) throws IOException {
        dfMapLock.writeLock().lock();
        try {
            checkBuildingSegment(df);

            NDataSegment newSegment = newSegment(df, segRange);

            Pair<Boolean, Boolean> pair = df.getSegments().fitInSegments(newSegment);
            if (pair.getFirst() == false || pair.getSecond() == false)
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

                newSegment.setSourcePartitionOffsetStart(toRefreshSeg.getSourcePartitionOffsetStart());
                newSegment.setSourcePartitionOffsetEnd(toRefreshSeg.getSourcePartitionOffsetEnd());
            }

            NDataflowUpdate upd = new NDataflowUpdate(df);
            upd.setToAddSegs(newSegment);
            updateDataflow(upd);

            return newSegment;
        } finally {
            dfMapLock.writeLock().unlock();
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
        NDataSegment lastSeg = df.getLastSegment();
        NDataSegment segment = new NDataSegment();
        segment.setId(lastSeg == null ? 0 : lastSeg.getId() + 1);
        segment.setName(Segments.makeSegmentName(null, segRange));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDataflow(df);
        segment.setStatus(SegmentStatusEnum.NEW);
        if (segRange != null) {
            segment.setSegRangeStart(segRange.start.v.toString());
            segment.setSegRangeEnd(segRange.end.v.toString());
        }
        segment.validate();
        return segment;
    }

    private void validateNewSegments(NDataflow df, NDataSegment newSegments) {
        List<NDataSegment> tobe = df.calculateToBeSegments(newSegments);
        List<NDataSegment> newList = Arrays.asList(newSegments);
        if (tobe.containsAll(newList) == false) {
            throw new IllegalStateException("For NDataflow " + df.getName() + ", the new segments " + newList
                    + " do not fit in its current " + df.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    public NDataflow reloadDataflowLocal(String dfName) {
        dfMapLock.readLock().lock();
        try {
            NDataflow df = reloadDataflowLocalAt(NDataflow.concatResourcePath(dfName));
            return df;
        } finally {
            dfMapLock.readLock().unlock();
        }
    }

    private NDataflow reloadDataflowLocalAt(String path) {
        ResourceStore store = getStore();
        NDataflow df;

        try {
            df = store.getResource(path, NDataflow.class, DATAFLOW_SERIALIZER);
            if (df == null) {
                throw new ResourceNotFoundException("No dataflow found at " + path);
            }

            String name = df.getName();
            checkState(StringUtils.isNotBlank(name), "NDataflow (at %s) name must not be blank", path);

            NCubePlan cubePlan = NCubePlanManager.getInstance(config).getCubePlan(df.getCubePlanName());
            checkNotNull(cubePlan, "NCubePlan '%s' (for NDataflow '%s') not found", df.getCubePlanName(), name);

            if (!cubePlan.getError().isEmpty()) {
                df.setStatus(RealizationStatusEnum.DESCBROKEN);
                logger.error("NCubePlan{} (for NDataflow '{}') is broken", cubePlan.getResourcePath(), name);
                for (String error : cubePlan.getError()) {
                    logger.error("Error: {}", error);
                }
            } else if (df.getStatus() == RealizationStatusEnum.DESCBROKEN) {
                df.setStatus(RealizationStatusEnum.DISABLED);
                logger.info("NDataflow {} changed from DESCBROKEN to DISABLED", name);
            }

            df.initAfterReload((KylinConfigExt) cubePlan.getConfig());
            dataflowMap.putLocal(name, df);

            logger.info("Reloaded NDataflow {} being {} having {} segments", name, df, df.getSegments().size());
            return df;

        } catch (Exception e) {
            logger.error("Error during load NDataflow, skipping : " + path, e);
            return null;
        }
    }

    public List<NDataflow> listAllDataflows() {
        dfMapLock.readLock().lock();
        try {
            return new ArrayList<>(dataflowMap.values());
        } finally {
            dfMapLock.readLock().unlock();
        }
    }

    public NDataflow getDataflow(String name) {
        dfMapLock.readLock().lock();
        try {
            return dataflowMap.get(name);
        } finally {
            dfMapLock.readLock().unlock();
        }
    }

    public NDataflow getDataflowByUuid(String uuid) {
        dfMapLock.readLock().lock();
        try {
            Collection<NDataflow> copy = new ArrayList<>(dataflowMap.values());
            for (NDataflow df : copy) {
                if (uuid.equals(df.getUuid()))
                    return df;
            }
            return null;
        } finally {
            dfMapLock.readLock().unlock();
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

    public NDataflow dropDataflow(String dfName, boolean deleteDesc) throws IOException {
        dfMapLock.writeLock().lock();
        try {
            logger.info("Dropping NDataflow '" + dfName + "'");
            // load projects before remove NDataflow from project

            // delete NDataflow and NCubePlan
            NDataflow df = getDataflow(dfName);

            // remove NDataflow and update cache
            getStore().deleteResource(df.getResourcePath());
            dataflowMap.remove(df.getName());

            if (deleteDesc && df.getCubePlanName() != null) {
                NCubePlanManager.getInstance(config).removeCubePlan(df.getCubePlan());
            }

            // delete NDataflow from project
            ProjectManager.getInstance(config).removeRealizationsFromProjects(getRealizationType(), dfName);

            return df;
        } finally {
            dfMapLock.writeLock().unlock();
        }
    }

    public NDataflow createDataflow(String dfName, String projectName, NCubePlan plan, String owner)
            throws IOException {
        dfMapLock.writeLock().lock();
        try {
            logger.info("Creating NDataflow '" + projectName + "-->" + dfName + "' from plan '" + plan.getName() + "'");

            // save NDataflow resource
            NDataflow df = NDataflow.create(dfName, plan);
            df.setOwner(owner);
            updateDataflowWithRetry(new NDataflowUpdate(df), 10);

            ProjectManager.getInstance(config).moveRealizationToProject(getRealizationType(), dfName, projectName,
                    owner);

            return df;
        } finally {
            dfMapLock.writeLock().unlock();
        }
    }

    public NDataflow createDataflow(NDataflow df, String projectName, String owner) throws IOException {
        dfMapLock.writeLock().lock();
        try {
            logger.info("Creating NDataflow '" + projectName + "-->" + df.getName() + "' from instance object.");

            // save NDataflow resource
            df.setOwner(owner);
            updateDataflowWithRetry(new NDataflowUpdate(df), 0);

            ProjectManager.getInstance(config).moveRealizationToProject(getRealizationType(), df.getName(), projectName,
                    owner);

            return df;
        } finally {
            dfMapLock.writeLock().unlock();
        }
    }

    public NDataflow updateDataflow(NDataflowUpdate update) throws IOException {
        dfMapLock.writeLock().lock();
        try {
            NDataflow df = updateDataflowWithRetry(update, 0);
            return df;
        } finally {
            dfMapLock.writeLock().unlock();
        }
    }

    private NDataflow updateDataflowWithRetry(NDataflowUpdate update, int retry) throws IOException {
        if (update == null || update.getDataflow() == null)
            throw new IllegalStateException();

        NDataflow df = update.getDataflow();
        logger.info("Updating NDataflow '" + df.getName() + "'");

        Segments<NDataSegment> newSegs = (Segments) df.getSegments().clone();

        if (update.getToAddSegs() != null)
            newSegs.addAll(Arrays.asList(update.getToAddSegs()));

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

        if (update.getToUpdateSegs() != null) {
            for (NDataSegment segment : update.getToUpdateSegs()) {
                for (int i = 0; i < newSegs.size(); i++) {
                    if (newSegs.get(i).getId() == segment.getId()) {
                        newSegs.set(i, segment);
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

        if (update.getOwner() != null) {
            df.setOwner(update.getOwner());
        }

        if (update.getCost() > 0) {
            df.setCost(update.getCost());
        }

        try {
            getStore().putResource(df.getResourcePath(), df, DATAFLOW_SERIALIZER);
            NDataSegDetailsManager.getInstance(df.getConfig()).updateDataflow(update);
        } catch (IllegalStateException ise) {
            logger.warn("Write conflict to update NDataflow " + df.getName() + " at try " + retry + ", will retry...");
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            df = reloadDataflowLocal(df.getName());
            update.setDataflow(df);
            retry++;
            df = updateDataflowWithRetry(update, retry);
        }

        // we are done, reload cache
        df = reloadDataflowLocal(df.getName());
        // broadcast update event
        dataflowMap.put(df.getName(), df);

        return df;
    }

    public void removeDataflowLocal(String dfName) {
        dfMapLock.writeLock().lock();
        try {
            NDataflow df = dataflowMap.get(dfName);
            if (df != null) {
                dataflowMap.removeLocal(dfName);
            }
        } finally {
            dfMapLock.writeLock().unlock();
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
                if (first.isOffsetCube()) {
                    hole.setSegRangeStart(first.getSegRangeEnd());
                    hole.setSegRangeEnd(second.getSegRangeStart());
                    hole.setSourcePartitionOffsetStart(first.getSourcePartitionOffsetEnd());
                    hole.setSourcePartitionOffsetEnd(second.getSourcePartitionOffsetStart());
                    hole.setName(Segments.makeSegmentName(null, hole.getSegRange()));
                } else {
                    hole.setTsRangeStart(first.getTsRangeEnd());
                    hole.setTsRangeStart(second.getTsRangeStart());
                    hole.setName(Segments.makeSegmentName(hole.getTSRange(), null));
                }
                holes.add(hole);
            }
        }
        return holes;
    }

}
