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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.kyligence.kap.metadata.NTableMetadataManager;


public class NDataLoadingRangeManager {
    private static final Logger logger = LoggerFactory.getLogger(NDataLoadingRangeManager.class);

    public static NDataLoadingRangeManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataLoadingRangeManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataLoadingRangeManager newInstance(KylinConfig conf, String project) throws IOException {
        return new NDataLoadingRangeManager(conf, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    // name => NDataLoadingRange
    private CaseInsensitiveStringCache<NDataLoadingRange> dataLoadingRangeMap;
    private CachedCrudAssist<NDataLoadingRange> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock rangeMapLock = new AutoReadWriteLock();

    public NDataLoadingRangeManager(KylinConfig config, String project) throws IOException {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) throws IOException {
        this.config = cfg;
        this.project = project;
        this.dataLoadingRangeMap = new CaseInsensitiveStringCache<>(config, project, "loading_range");
        String resourceRootPath = "/" + project + ResourceStore.DATA_LOADING_RANGE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataLoadingRange>(getStore(), resourceRootPath, NDataLoadingRange.class, dataLoadingRangeMap) {
            @Override
            protected NDataLoadingRange initEntityAfterReload(NDataLoadingRange dataLoadingRange, String resourceName) {
                // do nothing
                dataLoadingRange.setProject(project);
                return dataLoadingRange;
            }
        };

        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new NDataLoadingRangeSyncListener(), project, "loading_range");
    }

    private class NDataLoadingRangeSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock lock = rangeMapLock.lockForWrite()) {
                crud.reloadQuietly(cacheKey);
            }
            broadcaster.notifyProjectSchemaUpdate(project);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    // for test mostly
    public Serializer<NDataLoadingRange> getDataModelSerializer() {
        return crud.getSerializer();
    }

    public List<NDataLoadingRange> getDataLoadingRanges() {
        try (AutoLock lock = rangeMapLock.lockForRead()) {
            return new ArrayList<>(dataLoadingRangeMap.values());
        }
    }

    public NDataLoadingRange getDataLoadingRange(String name) {
        try (AutoLock lock = rangeMapLock.lockForRead()) {
            return dataLoadingRangeMap.get(name);
        }
    }

    private static String resourcePath(String project, String tableName) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.DATA_LOADING_RANGE_RESOURCE_ROOT)
                .append("/").append(tableName).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public NDataLoadingRange createDataLoadingRange(NDataLoadingRange dataLoadingRange) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            checkNDataLoadingRangeIdentify(dataLoadingRange);
            checkNDataLoadingRangeExist(dataLoadingRange);

            NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
            String tableName = dataLoadingRange.getTableName();
            TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
            if (tableDesc == null) {
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' 's table " + tableName + " does not exists");
            }
            String columnName = dataLoadingRange.getColumnName();
            ColumnDesc columnDesc = tableDesc.findColumnByName(columnName);
            if (columnDesc == null) {
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' 's column " + columnName + " does not exists");
            }
            String columnType = columnDesc.getDatatype();
            DataType dataType = DataType.getType(columnType);
            if (dataType == null || !dataType.isDate()) {
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' 's column " + columnName + " 's dataType does not support partition column");
            }

            return crud.save(dataLoadingRange);
        }
    }

    public NDataLoadingRange appendSegmentRange(NDataLoadingRange dataLoadingRange, SegmentRange segmentRange) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            NDataLoadingRange copyForWrite = copyForWrite(dataLoadingRange);
            List<SegmentRange> segmentRanges = copyForWrite.getSegmentRanges();
            if (CollectionUtils.isEmpty(segmentRanges)) {
                segmentRanges.add(segmentRange);
            } else {
                SegmentRange lastSegmentRange = segmentRanges.get(segmentRanges.size() - 1);
                SegmentRange firstSegmentRange = segmentRanges.get(0);

                if (lastSegmentRange.connects(segmentRange)) {
                    segmentRanges.add(segmentRange);
                } else if (segmentRange.connects(firstSegmentRange)) {
                    // if add segRange at first, waterMarkStart and waterMarkEnd ++
                    int waterMarkEnd = copyForWrite.getWaterMarkEnd();
                    int waterMarkStart = copyForWrite.getWaterMarkStart();
                    if (waterMarkEnd != -1) {
                        copyForWrite.setWaterMarkStart(++ waterMarkStart);
                        copyForWrite.setWaterMarkEnd(++ waterMarkEnd);
                    }
                    segmentRanges.add(0, segmentRange);
                }else {
                    throw new IllegalArgumentException("NDataLoadingRange appendSegmentRange " + segmentRange +
                            " has overlaps/gap with existing segmentRanges " + copyForWrite.getCoveredSegmentRange());
                }
            }
            return updateDataLoadingRange(copyForWrite);
        }
    }

    public NDataLoadingRange updateDataLoadingRange(NDataLoadingRange dataLoadingRange) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            if (getStore().getConfig().isCheckCopyOnWrite()) {
                if (dataLoadingRange.isCachedAndShared())
                    throw new IllegalStateException();
            }
            checkNDataLoadingRangeIdentify(dataLoadingRange);
            checkNDataLoadingRangeNotExist(dataLoadingRange);

            return crud.save(dataLoadingRange);
        }
    }

    public NDataLoadingRange copyForWrite(NDataLoadingRange dataLoadingRange) throws IOException {
        return crud.copyForWrite(dataLoadingRange);
    }

    public void removeDataLoadingRange(NDataLoadingRange dataLoadingRange) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            checkNDataLoadingRangeIdentify(dataLoadingRange);
            checkNDataLoadingRangeNotExist(dataLoadingRange);

            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(dataLoadingRange.getTableName());
            List<String> models = NDataModelManager.getInstance(config, project).getModelsUsingRootTable(tableDesc);
            if (CollectionUtils.isNotEmpty(models)) {
                throw new IllegalStateException("NDataLoadingRange is related in models '" + models + "' as rootFactTable, it can not be removed !!!");
            }
            crud.delete(dataLoadingRange);
        }
    }

    private void checkNDataLoadingRangeExist(NDataLoadingRange dataLoadingRange) {
        if (dataLoadingRangeMap.containsKey(dataLoadingRange.resourceName()))
            throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' has exist");
    }

    private void checkNDataLoadingRangeNotExist(NDataLoadingRange dataLoadingRange) {
        if (!dataLoadingRangeMap.containsKey(dataLoadingRange.resourceName()))
            throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' does not exist");
    }

    private void checkNDataLoadingRangeIdentify(NDataLoadingRange dataLoadingRange) {
        if (dataLoadingRange.getUuid() == null || StringUtils.isEmpty(dataLoadingRange.resourceName()))
            throw new IllegalArgumentException("NDataLoadingRange uuid or resourceName is empty");
    }

    public void updateDataLoadingRangeWaterMark(String tableName) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            NDataLoadingRange dataLoadingRange = getDataLoadingRange(tableName);
            if (dataLoadingRange == null)
                throw new IllegalArgumentException("NDataLoadingRange '" + tableName + "' does not exist");

            dataLoadingRange = copyForWrite(dataLoadingRange);

            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
            List<String> models = NDataModelManager.getInstance(config, project).getModelsUsingRootTable(tableDesc);
            boolean needUpdateWaterMark = false;

            if (CollectionUtils.isEmpty(models)) {
                dataLoadingRange.setActualQueryStart(Long.parseLong(dataLoadingRange.getCoveredSegmentRange().getStart().toString()));
                dataLoadingRange.setActualQueryEnd(Long.parseLong(dataLoadingRange.getCoveredSegmentRange().getEnd().toString()));
                updateDataLoadingRange(dataLoadingRange);
                return;
            } else {
                List<SegmentRange> segmentRanges = dataLoadingRange.getSegmentRanges();
                Pair<SegmentRange, SegmentRange> readySegmentRange = genReadySegmentRange(models);
                SegmentRange start = readySegmentRange.getFirst();
                SegmentRange end = readySegmentRange.getSecond();

                if (start != null) {
                    int waterMarkStart = segmentRanges.indexOf(start) >= 0 ? segmentRanges.indexOf(start) - 1 : -1;
                    if (waterMarkStart != dataLoadingRange.getWaterMarkStart()) {
                        dataLoadingRange.setWaterMarkStart(waterMarkStart);
                        needUpdateWaterMark = true;
                    }
                }
                if (end != null) {
                    int waterMarkEnd = segmentRanges.indexOf(end);
                    if (waterMarkEnd != dataLoadingRange.getWaterMarkEnd()) {
                        dataLoadingRange.setWaterMarkEnd(waterMarkEnd);
                        needUpdateWaterMark = true;
                    }
                }
            }

            if (needUpdateWaterMark) {
                updateActualQueryRange(dataLoadingRange);
            }
        }
    }

    private void updateActualQueryRange(NDataLoadingRange dataLoadingRange) throws IOException {
        if (dataLoadingRange.getWaterMarkEnd() == -1 && dataLoadingRange.getWaterMarkStart() == -1) {
            dataLoadingRange.setActualQueryStart(-1);
            dataLoadingRange.setActualQueryEnd(-1);
        } else {
            dataLoadingRange.setActualQueryStart(Long.parseLong(dataLoadingRange.getCoveredReadySegmentRange().getStart().toString()));
            dataLoadingRange.setActualQueryEnd(Long.parseLong(dataLoadingRange.getCoveredReadySegmentRange().getEnd().toString()));
        }
        updateDataLoadingRange(dataLoadingRange);
    }

    private Pair<SegmentRange, SegmentRange> genReadySegmentRange(List<String> models) {
        Pair<SegmentRange, SegmentRange> readySegmentRangePair = new Pair<>();
        if (CollectionUtils.isEmpty(models)) {
            return readySegmentRangePair;
        }
        SegmentRange first;
        SegmentRange last;
        for (String model : models) {
            List<NCubePlan> matchingCubePlans = NCubePlanManager.getInstance(config, project).findMatchingCubePlan(model, project, config);
            if (CollectionUtils.isEmpty(matchingCubePlans)) {
                continue;
            }
            for (NCubePlan cubePlan : matchingCubePlans) {
                NDataflow df = NDataflowManager.getInstance(config, project).getDataflow(cubePlan.getName());
                RealizationStatusEnum statusEnum = df.getStatus();
                if (!RealizationStatusEnum.READY.equals(statusEnum)) {
                    continue;
                }
                List<SegmentRange> readySegmentRangeList = calcReadySegmentRangeList(df);
                if (CollectionUtils.isEmpty(readySegmentRangeList)) {
                    return new Pair<>();
                }
                int size = readySegmentRangeList.size();
                first = readySegmentRangeList.get(0);
                last = readySegmentRangeList.get(size - 1);
                SegmentRange firstReady = readySegmentRangePair.getFirst();
                SegmentRange lastReady = readySegmentRangePair.getSecond();

                if (firstReady == null) {
                    readySegmentRangePair.setFirst(first);
                } else {
                    if (first.compareTo(firstReady) > 0) {
                        readySegmentRangePair.setFirst(first);
                    }
                }

                if (lastReady == null) {
                    readySegmentRangePair.setSecond(last);
                } else {
                    if (last.compareTo(lastReady) < 0) {
                        readySegmentRangePair.setSecond(last);
                    }
                }
            }
        }
        return readySegmentRangePair;
    }

    private List<SegmentRange> calcReadySegmentRangeList(NDataflow df) {
        List<SegmentRange> segmentRangeList = Lists.newArrayList();
        if (df == null) {
            return segmentRangeList;
        }
        Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
        if (CollectionUtils.isEmpty(readySegments)) {
            return segmentRangeList;
        }
        for (NDataSegment readySegment : readySegments) {
            segmentRangeList.add(readySegment.getSegRange());
        }
        Collections.sort(segmentRangeList);
        return segmentRangeList;

    }

}
