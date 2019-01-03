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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.var;

public class NDataLoadingRangeManager {
    private static final Logger logger = LoggerFactory.getLogger(NDataLoadingRangeManager.class);
    private static final String DATA_LOADING_RANGE = "NDataLoadingRange '";

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

    private CachedCrudAssist<NDataLoadingRange> crud;

    public NDataLoadingRangeManager(KylinConfig config, String project) throws IOException {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) {
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.DATA_LOADING_RANGE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataLoadingRange>(getStore(), resourceRootPath, NDataLoadingRange.class) {
            @Override
            protected NDataLoadingRange initEntityAfterReload(NDataLoadingRange dataLoadingRange, String resourceName) {
                dataLoadingRange.initAfterReload(config, project);
                return dataLoadingRange;
            }
        };

        crud.reloadAll();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    // for test mostly
    public Serializer<NDataLoadingRange> getDataLoadingRangeSerializer() {
        return crud.getSerializer();
    }

    public List<NDataLoadingRange> getDataLoadingRanges() {
        return crud.listAll();
    }

    public NDataLoadingRange getDataLoadingRange(String name) {
        return crud.get(name);
    }

    private static String resourcePath(String project, String tableName) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.DATA_LOADING_RANGE_RESOURCE_ROOT)
                .append("/").append(tableName).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public NDataLoadingRange createDataLoadingRange(NDataLoadingRange dataLoadingRange) {
        checkNDataLoadingRangeIdentify(dataLoadingRange);
        checkNDataLoadingRangeExist(dataLoadingRange);

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
        String tableName = dataLoadingRange.getTableName();
        TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException(DATA_LOADING_RANGE + dataLoadingRange.resourceName() + "' 's table "
                    + tableName + " does not exists");
        }
        String columnName = dataLoadingRange.getColumnName();
        ColumnDesc columnDesc = tableDesc.findColumnByName(columnName);
        if (columnDesc == null) {
            throw new IllegalArgumentException(DATA_LOADING_RANGE + dataLoadingRange.resourceName() + "' 's column "
                    + columnName + " does not exists");
        }
        String columnType = columnDesc.getDatatype();
        DataType dataType = DataType.getType(columnType);
        if (dataType == null || !dataType.isLegalPartitionColumnType()) {
            throw new IllegalArgumentException(DATA_LOADING_RANGE + dataLoadingRange.resourceName() + "' 's column "
                    + columnName + " 's dataType does not support partition column");
        }

        return crud.save(dataLoadingRange);
    }

    public NDataLoadingRange appendSegmentRange(NDataLoadingRange dataLoadingRange, SegmentRange segmentRange) {
        NDataLoadingRange copyForWrite = copyForWrite(dataLoadingRange);
        val coveredRange = copyForWrite.getCoveredRange();
        if (coveredRange == null) {
            copyForWrite.setCoveredRange(segmentRange);
        } else {
            if (coveredRange.connects(segmentRange)) {
                copyForWrite.setCoveredRange(coveredRange.coverWith(segmentRange));
            } else if (segmentRange.connects(coveredRange)) {
                copyForWrite.setCoveredRange(segmentRange.coverWith(coveredRange));
            } else {
                throw new IllegalArgumentException("NDataLoadingRange appendSegmentRange " + segmentRange
                        + " has overlaps/gap with existing segmentRanges " + copyForWrite.getCoveredRange());
            }
        }
        return updateDataLoadingRange(copyForWrite);
    }

    public NDataLoadingRange updateDataLoadingRange(NDataLoadingRange dataLoadingRange) {
        if (getStore().getConfig().isCheckCopyOnWrite()) {
            if (dataLoadingRange.isCachedAndShared())
                throw new IllegalStateException();
        }
        checkNDataLoadingRangeIdentify(dataLoadingRange);
        checkNDataLoadingRangeNotExist(dataLoadingRange);

        return crud.save(dataLoadingRange);
    }

    public NDataLoadingRange copyForWrite(NDataLoadingRange dataLoadingRange) {
        return crud.copyForWrite(dataLoadingRange);
    }

    public void removeDataLoadingRange(NDataLoadingRange dataLoadingRange) {
        checkNDataLoadingRangeIdentify(dataLoadingRange);
        checkNDataLoadingRangeNotExist(dataLoadingRange);
        crud.delete(dataLoadingRange);
    }

    private void checkNDataLoadingRangeExist(NDataLoadingRange dataLoadingRange) {
        if (crud.contains(dataLoadingRange.resourceName()))
            throw new IllegalArgumentException(DATA_LOADING_RANGE + dataLoadingRange.resourceName() + "' has exist");
    }

    private void checkNDataLoadingRangeNotExist(NDataLoadingRange dataLoadingRange) {
        if (!crud.contains(dataLoadingRange.resourceName()))
            throw new IllegalArgumentException(
                    DATA_LOADING_RANGE + dataLoadingRange.resourceName() + "' does not exist");
    }

    private void checkNDataLoadingRangeIdentify(NDataLoadingRange dataLoadingRange) {
        if (dataLoadingRange.getUuid() == null || StringUtils.isEmpty(dataLoadingRange.resourceName()))
            throw new IllegalArgumentException("NDataLoadingRange uuid or resourceName is empty");
    }

    private List<NDataflow> getOnlineDataflow(NDataLoadingRange dataLoadingRange) {
        val dfManager = NDataflowManager.getInstance(config, project);
        return dfManager.getDataflowsByTableAndStatus(dataLoadingRange.getTableName(), RealizationStatusEnum.ONLINE);
    }

    public SegmentRange getQuerableSegmentRange(NDataLoadingRange dataLoadingRange) {

        val dataflows = getOnlineDataflow(dataLoadingRange);

        var querableRange = dataLoadingRange.getCoveredRange();
        if (CollectionUtils.isEmpty(dataflows)) {
            return dataLoadingRange.getCoveredRange();
        }
        for (val dataflow : dataflows) {
            if (querableRange == null) {
                break;
            }
            querableRange = getOverlapRange(dataflow, querableRange);
        }
        return querableRange;
    }

    private SegmentRange getOverlapRange(NDataflow dataflow, SegmentRange querableRange) {
        val readySegments = dataflow.getSegments(SegmentStatusEnum.READY);
        if (CollectionUtils.isEmpty(readySegments)) {
            return null;
        }
        val readySegRange = readySegments.getFirstSegment().getSegRange().coverWith(readySegments.getLastSegment().getSegRange());
        return readySegRange.getOverlapRange(querableRange);

    }

    public List<SegmentRange> getSegRangesToBuildForNewDataflow(NDataLoadingRange dataLoadingRange) {
        val standardDataflow = getStandardDataflow(dataLoadingRange);

        if (standardDataflow != null) {
            return standardDataflow.getFlatSegments().getSegRanges();
        }
        if (dataLoadingRange.getCoveredRange() == null) {
            return null;
        }
        val segConfig = NSegmentConfigHelper.getTableSegmentConfig(project, dataLoadingRange.getTableName());
        if (!segConfig.getAutoMergeEnabled()) {
            return Lists.newArrayList(dataLoadingRange.getCoveredRange());
        }
        return Segments.getSplitedSegRanges(dataLoadingRange.getCoveredRange(), segConfig.getAutoMergeTimeRanges(), segConfig.getVolatileRange());

    }

    private NDataflow getStandardDataflow(NDataLoadingRange dataLoadingRange) {
        val dataflows = getOnlineDataflow(dataLoadingRange);
        NDataflow candidateDataflow = null;
        if (CollectionUtils.isEmpty(dataflows)) {
            return null;
        }
        var minSize = Integer.MAX_VALUE;

        for (val df : dataflows) {
            if (CollectionUtils.isEmpty(df.getFlatSegments())) {
                continue;
            }
            val flatSegs = df.getFlatSegments();
            val flatSegRange = flatSegs.getFirstSegment().getSegRange().coverWith(flatSegs.getLastSegment().getSegRange());
            if (!flatSegRange.startStartMatch(dataLoadingRange.getCoveredRange()) || !flatSegRange.endEndMatch(dataLoadingRange.getCoveredRange())) {
                continue;
            }
            val size = df.getFlatSegments().size();
            minSize = Integer.min(minSize, size);
            if (size == minSize) {
                candidateDataflow = df;
            }
        }
        return candidateDataflow;
    }

    public void updateCoveredRangeAfterRetention(NDataModel model, NDataSegment lastSegment) {
        if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
            return;
        }
        val loadingRange = getDataLoadingRange(model.getRootFactTableName());
        if (loadingRange == null) {
            return;
        } else {
            val copy = copyForWrite(loadingRange);
            val coveredRange = copy.getCoveredRange();
            if (coveredRange == null) {
                return;
            }
            if (lastSegment.getSegRange().overlaps(coveredRange)) {
                copy.setCoveredRange(lastSegment.getSegRange().getEndDeviation(coveredRange));
                updateDataLoadingRange(copy);
            }

        }
    }
}
