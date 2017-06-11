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

package io.kyligence.kap.cube.raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class RawTableManager implements IRealizationProvider {

    private static final Logger logger = LoggerFactory.getLogger(RawTableManager.class);

    public static final Serializer<RawTableInstance> INSTANCE_SERIALIZER = new JsonSerializer<RawTableInstance>(
            RawTableInstance.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, RawTableManager> CACHE = new ConcurrentHashMap<>();

    public static RawTableManager getInstance(KylinConfig config) {
        RawTableManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (RawTableManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new RawTableManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init RawTableManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ==========================================================

    private KylinConfig config;
    // name ==> RawTableDesc
    private CaseInsensitiveStringCache<RawTableInstance> rawTableInstanceMap;

    private RawTableManager(KylinConfig config) throws IOException {
        logger.info("Initializing RawTableManager with config " + config);
        this.config = config;
        this.rawTableInstanceMap = new CaseInsensitiveStringCache<RawTableInstance>(config, "raw_table");

        // touch lower level metadata before registering my listener
        reloadAllRawTableInstance();
        Broadcaster.getInstance(config).registerListener(new RawTableSyncListener(), "raw_table");
        Broadcaster.getInstance(config).registerListener(new RawTableCubeSyncListener(), "cube");
    }

    private class RawTableSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof RawTableInstance) {
                    reloadRawTableInstanceLocal(real.getName());
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {

            if (event == Event.DROP)
                return;

            String rawTableName = cacheKey;

            reloadRawTableInstanceLocal(rawTableName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.INVERTED_INDEX,
                    rawTableName)) {
                broadcaster.notifyProjectDataUpdate(prj.getName());
            }
        }
    }

    private class RawTableCubeSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String cubeName = cacheKey;

            if (event == Event.DROP)
                return;

            //By design. Make rawtable be consistence with cube, but only in cache. In merge step, rawtable' segment info in hbase is still useful.
            if (rawTableInstanceMap.containsKey(cubeName)) {
                reloadRawTableInstanceLocal(cubeName);
            }
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public RawTableInstance getAccompanyRawTable(CubeInstance cube) {
        return getRawTableInstance(cube.getName());
    }

    public RawTableInstance getRawTableInstance(String name) {
        return rawTableInstanceMap.get(name);
    }

    /**
     * Reload RawTableInstance from resource store. Triggered by an instance update event.
     */
    public RawTableInstance reloadRawTableInstanceLocal(String name) throws IOException {

        // Save Source
        String path = RawTableInstance.concatResourcePath(name);

        // Reload the RawTableInstance
        RawTableInstance instance = loadRawTableInstance(path);

        // Keep consistence with cube
        instance.validateSegments();

        // Here replace the old one
        rawTableInstanceMap.putLocal(instance.getName(), instance);
        return instance;
    }

    public List<RawTableInstance> listAllRawTables() {
        return new ArrayList<RawTableInstance>(rawTableInstanceMap.values());
    }

    private RawTableInstance loadRawTableInstance(String path) throws IOException {
        ResourceStore store = getStore();
        RawTableInstance instance = store.getResource(path, RawTableInstance.class, INSTANCE_SERIALIZER);

        instance.init(config);

        if (StringUtils.isBlank(instance.getName())) {
            throw new IllegalStateException("RawTable name must not be blank");
        }

        return instance;
    }

    // sync on update
    public RawTableInstance createRawTableInstance(String cubeName, String projectName, RawTableDesc desc, String owner)
            throws IOException {
        logger.info("Creating rawtable '" + projectName + "-->" + cubeName + "' from desc '" + desc.getName() + "'");
        // save rawtable resource
        RawTableInstance raw = RawTableInstance.create(cubeName, desc);
        raw.setOwner(owner);
        raw.init(desc.getConfig());
        updateRawTable(new RawTableUpdate(raw));
        ProjectManager.getInstance(config).moveRealizationToProject(RealizationType.INVERTED_INDEX, cubeName,
                projectName, owner);
        return raw;
    }

    // sync on update
    public RawTableInstance dropRawTableInstance(String cubeName, boolean deleteDesc) throws IOException {
        logger.info("Dropping rawtable '" + cubeName + "'");

        RawTableInstance raw = getRawTableInstance(cubeName.toLowerCase());

        if (deleteDesc && raw.getRawTableDesc() != null) {
            RawTableDescManager.getInstance(config).removeRawTableDesc(raw.getRawTableDesc());
        }

        getStore().deleteResource(raw.getResourcePath());
        this.rawTableInstanceMap.remove(raw.getName());

        ProjectManager.getInstance(config).removeRealizationsFromProjects(RealizationType.INVERTED_INDEX, cubeName);

        return raw;
    }

    public void removeRawTableInstanceLocal(String name) throws IOException {
        rawTableInstanceMap.removeLocal(name);
    }

    void reloadAllRawTableInstance() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading RawTableInstance from folder "
                + store.getReadableResourcePath(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT));

        rawTableInstanceMap.clear();

        List<String> paths = store.collectResourceRecursively(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            RawTableInstance instance;
            try {
                instance = loadRawTableInstance(path);
            } catch (Exception e) {
                logger.error("Error loading RawTableInstance " + path, e);
                continue;
            }
            if (path.equals(instance.getResourcePath()) == false) {
                logger.error("Skip suspicious instance at " + path + ", " + instance + " should be at "
                        + instance.getResourcePath());
                continue;
            }
            if (rawTableInstanceMap.containsKey(instance.getName())) {
                logger.error("Dup RawTableInstance name '" + instance.getName() + "' on path " + path);
                continue;
            }
            instance.validateSegments();
            rawTableInstanceMap.putLocal(instance.getName(), instance);
        }

        logger.debug("Loaded " + rawTableInstanceMap.size() + " RawTableInstance(s)");
    }

    public RawTableSegment appendSegment(RawTableInstance instance, CubeSegment seg) throws IOException {
        RawTableSegment segment = new RawTableSegment(instance);
        // TODO: segment.setUuid(UUID.randomUUID().toString());
        segment.setUuid(seg.getUuid());
        segment.setName(seg.getName());
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDateRangeStart(seg.getDateRangeStart());
        segment.setDateRangeEnd(seg.getDateRangeEnd());
        segment.setSourceOffsetStart(
                seg.getSourceOffsetStart() == seg.getDateRangeStart() ? 0 : seg.getSourceOffsetStart());
        segment.setSourceOffsetEnd(seg.getSourceOffsetEnd() == seg.getDateRangeEnd() ? 0 : seg.getSourceOffsetEnd());
        segment.setStatus(SegmentStatusEnum.NEW);

        RawTableUpdate builder = new RawTableUpdate(instance);
        builder.setToAddSegs(segment);
        updateRawTable(builder);
        return segment;
    }

    public void promoteNewlyBuiltSegments(RawTableInstance raw, RawTableSegment newSegment) throws IOException {
        if (StringUtils.isBlank(newSegment.getLastBuildJobID()))
            throw new IllegalStateException("For raw " + raw + ", segment " + newSegment + " missing LastBuildJobID");

        if (isReady(newSegment) == true) {
            logger.warn("For raw " + raw + ", segment " + newSegment + " state should be NEW but is READY");
        }

        List<RawTableSegment> tobe = raw.calculateToBeSegments(newSegment);

        if (tobe.contains(newSegment) == false)
            throw new IllegalStateException(
                    "For raw " + raw + ", segment " + newSegment + " is expected but not in the tobe " + tobe);

        newSegment.setStatus(SegmentStatusEnum.READY);

        List<RawTableSegment> toRemoveSegs = Lists.newArrayList();
        for (RawTableSegment segment : raw.getSegments()) {
            if (!tobe.contains(segment))
                toRemoveSegs.add(segment);
        }

        logger.info(
                "Promoting rawtable " + raw + ", new segments " + newSegment + ", to remove segments " + toRemoveSegs);

        RawTableUpdate rawBuilder = new RawTableUpdate(raw);
        rawBuilder.setToRemoveSegs(toRemoveSegs.toArray(new RawTableSegment[toRemoveSegs.size()]))
                .setToUpdateSegs(newSegment).setStatus(RealizationStatusEnum.READY);
        updateRawTable(rawBuilder);
    }

    public List<RawTableSegment> getRawtableSegmentByDataRange(RawTableInstance raw, long startDate, long endDate) {
        LinkedList<RawTableSegment> result = Lists.newLinkedList();
        for (RawTableSegment seg : raw.getSegments()) {
            if (startDate <= seg.getDateRangeStart() && seg.getDateRangeEnd() <= endDate) {
                result.add(seg);
            }
        }
        return result;
    }

    private boolean isReady(RawTableSegment seg) {
        return seg.getStatus() == SegmentStatusEnum.READY;
    }

    public RawTableSegment mergeSegments(RawTableInstance raw, String cubeSegUuid, long startDate, long endDate,
            long startOffset, long endOffset, boolean force) throws IOException {
        if (raw.getSegments().isEmpty())
            throw new IllegalArgumentException("RawTable " + raw + " has no segments");
        if (startDate >= endDate && startOffset >= endOffset)
            throw new IllegalArgumentException("Invalid merge range");

        checkNoBuildingSegment(raw);
        checkCubeIsPartitioned(raw);

        boolean isOffsetsOn = raw.getSegments().get(0).isSourceOffsetsOn();

        if (isOffsetsOn) {
            // offset cube, merge by date range?
            if (startOffset == endOffset) {
                Pair<RawTableSegment, RawTableSegment> pair = raw.getSegments(SegmentStatusEnum.READY)
                        .findMergeOffsetsByDateRange(startDate, endDate, Long.MAX_VALUE);
                if (pair == null)
                    throw new IllegalArgumentException("Find no segments to merge by date range " + startDate + "-"
                            + endDate + " for rawtable " + raw);
                startOffset = pair.getFirst().getSourceOffsetStart();
                endOffset = pair.getSecond().getSourceOffsetEnd();
            }
            startDate = 0;
            endDate = 0;
        } else {
            // date range cube, make sure range is on dates
            if (startDate == endDate) {
                startDate = startOffset;
                endDate = endOffset;
            }
            startOffset = 0;
            endOffset = 0;
        }

        RawTableSegment newSegment = newSegment(raw, cubeSegUuid, startDate, endDate, startOffset, endOffset);

        List<RawTableSegment> mergingSegments = raw.getMergingSegments(newSegment);
        if (mergingSegments.size() <= 1)
            throw new IllegalArgumentException(
                    "Range " + newSegment.getSourceOffsetStart() + "-" + newSegment.getSourceOffsetEnd()
                            + " must contain at least 2 segments, but there is " + mergingSegments.size());

        RawTableSegment first = mergingSegments.get(0);
        RawTableSegment last = mergingSegments.get(mergingSegments.size() - 1);
        if (newSegment.isSourceOffsetsOn()) {
            newSegment.setDateRangeStart(minDateRangeStart(mergingSegments));
            newSegment.setDateRangeEnd(maxDateRangeEnd(mergingSegments));
            newSegment.setSourceOffsetStart(first.getSourceOffsetStart());
            newSegment.setSourceOffsetEnd(last.getSourceOffsetEnd());
        } else {
            newSegment.setDateRangeStart(first.getSourceOffsetStart());
            newSegment.setDateRangeEnd(last.getSourceOffsetEnd());
        }

        if (force == false) {
            List<String> emptySegment = Lists.newArrayList();
            for (RawTableSegment seg : mergingSegments) {
                if (seg.getSizeKB() == 0) {
                    emptySegment.add(seg.getName());
                }
            }

            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException(
                        "Empty rawtable segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: "
                                + emptySegment);
            }
        }

        validateNewSegments(raw, newSegment);

        RawTableUpdate builder = new RawTableUpdate(raw);
        builder.setToAddSegs(newSegment);
        updateRawTable(builder);

        return newSegment;
    }

    // for test
    private RawTableSegment newSegment(RawTableInstance raw, String cubeSegUuid, long startDate, long endDate,
            long startOffset, long endOffset) {
        RawTableSegment segment = new RawTableSegment(raw);
        segment.setUuid(null == cubeSegUuid ? UUID.randomUUID().toString() : cubeSegUuid);
        segment.setName(RawTableSegment.makeSegmentName(startDate, endDate, startOffset, endOffset));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setDateRangeStart(startDate);
        segment.setDateRangeEnd(endDate);
        segment.setSourceOffsetStart(startOffset);
        segment.setSourceOffsetEnd(endOffset);
        segment.setStatus(SegmentStatusEnum.NEW);
        segment.validate();
        return segment;
    }

    public void validateNewSegments(RawTableInstance raw, RawTableSegment newSegment) {
        List<RawTableSegment> tobe = raw.calculateToBeSegments(newSegment);
        List<RawTableSegment> newList = Arrays.asList(newSegment);
        if (tobe.containsAll(newList) == false) {
            throw new IllegalStateException("For rawtable " + raw + ", the new segments " + newList
                    + " do not fit in its current " + raw.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    private void checkNoBuildingSegment(RawTableInstance raw) {
        if (raw.getBuildingSegments().size() > 0) {
            throw new IllegalStateException("There is already a building segment!");
        }
    }

    private void checkCubeIsPartitioned(RawTableInstance raw) {
        if (raw.getModel().getPartitionDesc().isPartitioned() == false) {
            throw new IllegalStateException(
                    "there is no partition date column specified, only full build is supported");
        }
    }

    private long minDateRangeStart(List<RawTableSegment> mergingSegments) {
        long min = Long.MAX_VALUE;
        for (RawTableSegment seg : mergingSegments)
            min = Math.min(min, seg.getDateRangeStart());
        return min;
    }

    private long maxDateRangeEnd(List<RawTableSegment> mergingSegments) {
        long max = Long.MIN_VALUE;
        for (RawTableSegment seg : mergingSegments)
            max = Math.max(max, seg.getDateRangeEnd());
        return max;
    }

    public List<RawTableInstance> getRawTablesByDesc(String descName) {

        descName = descName.toUpperCase();
        List<RawTableInstance> list = this.listAllRawTables();
        List<RawTableInstance> result = new ArrayList<RawTableInstance>();
        Iterator<RawTableInstance> it = list.iterator();
        while (it.hasNext()) {
            RawTableInstance ci = it.next();
            if (descName.equalsIgnoreCase(ci.getDescName())) {
                result.add(ci);
            }
        }
        return result;
    }

    public RawTableInstance updateRawTable(RawTableUpdate update) throws IOException {
        return updateRawTableWithRetry(update, 0);
    }

    public RawTableInstance updateRawTableWithRetry(RawTableUpdate update, int retry) throws IOException {
        if (update == null || update.getRawTableInstance() == null)
            throw new IllegalStateException();

        RawTableInstance raw = update.getRawTableInstance();
        logger.info("Updating rawtable instance '" + raw.getName() + "'");

        Segments<RawTableSegment> newSegs = (Segments) (raw.getSegments().clone());

        if (update.getToAddSegs() != null)
            newSegs.addAll(Arrays.asList(update.getToAddSegs()));

        List<String> toRemoveResources = Lists.newArrayList();
        if (update.getToRemoveSegs() != null) {
            Iterator<RawTableSegment> iterator = newSegs.iterator();
            while (iterator.hasNext()) {
                RawTableSegment currentSeg = iterator.next();
                boolean found = false;
                for (RawTableSegment toRemoveSeg : update.getToRemoveSegs()) {
                    if (currentSeg.getUuid().equals(toRemoveSeg.getUuid())) {
                        iterator.remove();
                        toRemoveResources.add(toRemoveSeg.getStatisticsResourcePath());
                        found = true;
                    }
                }
                if (found == false) {
                    logger.error("Segment '" + currentSeg.getName() + "' doesn't exist for remove.");
                }
            }

        }

        if (update.getToUpdateSegs() != null) {
            for (RawTableSegment segment : update.getToUpdateSegs()) {
                boolean found = false;
                for (int i = 0; i < newSegs.size(); i++) {
                    if (newSegs.get(i).getUuid().equals(segment.getUuid())) {
                        newSegs.set(i, segment);
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    logger.error("Segment '" + segment.getName() + "' doesn't exist for update.");
                }
            }
        }

        Collections.sort(newSegs);
        RawTableValidator.validate(newSegs);
        raw.setSegments(newSegs);

        if (update.getStatus() != null) {
            raw.setStatus(update.getStatus());
        }

        if (update.getOwner() != null) {
            raw.setOwner(update.getOwner());
        }

        if (update.getCost() > 0) {
            raw.setCost(update.getCost());
        }

        try {
            getStore().putResource(raw.getResourcePath(), raw, INSTANCE_SERIALIZER);
        } catch (IllegalStateException ise) {
            logger.warn("Write conflict to update rawtable" + raw.getName() + " at try " + retry + ", will retry...");
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            raw = reloadRawTableInstanceLocal(raw.getName());
            update.setRawTableInstance(raw);
            retry++;
            raw = updateRawTableWithRetry(update, retry);
        }

        if (toRemoveResources.size() > 0) {
            for (String resource : toRemoveResources) {
                try {
                    getStore().deleteResource(resource);
                } catch (IOException ioe) {
                    logger.error("Failed to delete resource " + toRemoveResources.toString());
                }
            }
        }

        raw.validateSegments();

        this.rawTableInstanceMap.put(raw.getName(), raw);

        //this is a duplicate call to take care of scenarios where REST cache service unavailable
        ProjectManager.getInstance(raw.getConfig()).clearL2Cache();

        return raw;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.INVERTED_INDEX;
    }

    @Override
    public IRealization getRealization(String name) {
        return this.rawTableInstanceMap.get(name.toUpperCase());
    }
}
