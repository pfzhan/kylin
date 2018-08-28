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
import java.util.List;

import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
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
            if (dataLoadingRange.getUuid() == null || StringUtils.isEmpty(dataLoadingRange.resourceName()))
                throw new IllegalArgumentException();
            if (dataLoadingRangeMap.containsKey(dataLoadingRange.resourceName()))
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' already exists");

            NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
            String tableName = dataLoadingRange.getTableName();
            TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
            if (tableDesc == null) {
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' 's table " + tableName + "does not exists");
            }
            String columnName = dataLoadingRange.getColumnName();
            ColumnDesc columnDesc = tableDesc.findColumnByName(columnName);
            if (columnDesc == null) {
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' 's column " + columnName + " does not exists");
            }

            return crud.save(dataLoadingRange);
        }
    }

    public NDataLoadingRange updateDataLoadingRange(NDataLoadingRange dataLoadingRange) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            if (dataLoadingRange.getUuid() == null || StringUtils.isEmpty(dataLoadingRange.resourceName()))
                throw new IllegalArgumentException();
            if (!dataLoadingRangeMap.containsKey(dataLoadingRange.resourceName()))
                throw new IllegalArgumentException("NDataLoadingRange '" + dataLoadingRange.resourceName() + "' does not exist");

            return crud.save(dataLoadingRange);
        }
    }

    public void updateDataLoadingRangeWaterMark(String tableName, Long waterMark) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            NDataLoadingRange dataLoadingRange = getDataLoadingRange(tableName);
            if (dataLoadingRange == null)
                throw new IllegalArgumentException("NDataLoadingRange '" + tableName + "' does not exist");

            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
            List<String> models = NDataModelManager.getInstance(config, project).getModelsUsingTable(tableDesc);
            for (String model : models) {
                List<NCubePlan> matchingCubePlans = NCubePlanManager.getInstance(config, project).findMatchingCubePlan(model, project, config);
                if (CollectionUtils.isEmpty(matchingCubePlans)) {
                    continue;
                }
                for (NCubePlan cubePlan : matchingCubePlans) {
                    NDataflow df = NDataflowManager.getInstance(config, project).getDataflow(cubePlan.getName());
                    NDataSegment segment = df.getSegments().getLatestReadySegment();
                    if (segment == null) {
                        return;
                    }
                    SegmentRange segmentRange = segment.getSegRange();
                    if (segmentRange == null) {
                        return;
                    }
                    long end = (Long) segmentRange.getEnd();
                    if (end < waterMark) {
                        return;
                    }
                }
            }

            SegmentRange.TimePartitionedDataLoadingRange loadingRange = (SegmentRange.TimePartitionedDataLoadingRange) dataLoadingRange.getDataLoadingRange();
            long currentWaterMark = loadingRange.getWaterMark();

            if (waterMark > currentWaterMark) {
                loadingRange.setWaterMark(waterMark);
                crud.save(dataLoadingRange);
            }
        }
    }

    public void updateDataLoadingRangeWaterMark(String tableName) throws IOException {
        try (AutoLock lock = rangeMapLock.lockForWrite()) {
            NDataLoadingRange dataLoadingRange = getDataLoadingRange(tableName);
            if (dataLoadingRange == null)
                throw new IllegalArgumentException("NDataLoadingRange '" + tableName + "' does not exist");

            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
            List<String> models = NDataModelManager.getInstance(config, project).getModelsUsingTable(tableDesc);
            long newWaterMark = 0L;

            for (String model : models) {
                List<NCubePlan> matchingCubePlans = NCubePlanManager.getInstance(config, project).findMatchingCubePlan(model, project, config);
                if (CollectionUtils.isEmpty(matchingCubePlans)) {
                    continue;
                }
                for (NCubePlan cubePlan : matchingCubePlans) {
                    NDataflow df = NDataflowManager.getInstance(config, project).getDataflow(cubePlan.getName());
                    NDataSegment segment = df.getSegments().getLatestReadySegment();
                    if (segment == null) {
                        return;
                    }
                    SegmentRange segmentRange = segment.getSegRange();
                    if (segmentRange == null) {
                        return;
                    }
                    long end = (Long) segmentRange.getEnd();
                    newWaterMark = Math.min(end, newWaterMark);
                }
            }

            SegmentRange.TimePartitionedDataLoadingRange loadingRange = (SegmentRange.TimePartitionedDataLoadingRange) dataLoadingRange.getDataLoadingRange();
            long currentWaterMark = loadingRange.getWaterMark();

            if (newWaterMark > currentWaterMark) {
                loadingRange.setWaterMark(newWaterMark);
                crud.save(dataLoadingRange);
            }
        }
    }

}
