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
package io.kyligence.kap.rest.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.constant.Constants;
import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.SegmentPartition;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SourceUsageService extends BasicService {

    private long calculateTableInputBytes(SourceUsageRecord.TableCapacityDetail tableDetail) {
        long sumBytes = 0;
        for (SourceUsageRecord.ColumnCapacityDetail column : tableDetail.getColumns()) {
            sumBytes += column.getMaxSourceBytes();
        }
        return sumBytes;
    }

    private long getLookupTableSource(SourceUsageRecord.TableCapacityDetail table,
            SourceUsageRecord.ProjectCapacityDetail project, long inputBytes) {
        String projectName = project.getName();
        String tableName = table.getName();
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                projectName);
        TableDesc tableDesc = tableManager.getTableDesc(tableName);
        if (tableManager.existsSnapshotTableByName(tableName)) {
            TableExtDesc tableExtDesc = tableManager.getOrCreateTableExt(tableDesc);
            long originalSize = tableExtDesc.getOriginalSize();
            if (originalSize == -1) {
                // for 4.1 upgrade to 4.2
                return 0;
            }
            return originalSize;
        } else {
            return inputBytes;
        }
    }

    private void checkTableKind(SourceUsageRecord.TableCapacityDetail tableDetail, NDataModel model) {
        String tableName = tableDetail.getName();
        if (tableName.equals(model.getRootFactTableName())
                && SourceUsageRecord.TableKind.FACT != tableDetail.getTableKind()) {
            tableDetail.setTableKind(SourceUsageRecord.TableKind.FACT);
        } else if (model.isLookupTable(tableName) && SourceUsageRecord.TableKind.FACT != tableDetail.getTableKind()) {
            tableDetail.setTableKind(SourceUsageRecord.TableKind.WITHSNAP);
        } else if (model.isFactTable(tableName) && SourceUsageRecord.TableKind.FACT != tableDetail.getTableKind()
                && SourceUsageRecord.TableKind.WITHSNAP != tableDetail.getTableKind()) {
            tableDetail.setTableKind(SourceUsageRecord.TableKind.WITHOUTSNAP);
        }
    }

    private long calculateTableCapacity(SourceUsageRecord.TableCapacityDetail table,
            SourceUsageRecord.ProjectCapacityDetail project) {
        long inputBytes = calculateTableInputBytes(table);

        long sourceBytes;
        if (SourceUsageRecord.TableKind.FACT == table.getTableKind()) {
            return inputBytes;
        } else {
            sourceBytes = getLookupTableSource(table, project, inputBytes);
        }
        return sourceBytes;
    }

    private void updateProjectSourceUsage(SourceUsageRecord.ProjectCapacityDetail project) {
        long sum = 0L;
        SourceUsageRecord.CapacityStatus status = SourceUsageRecord.CapacityStatus.OK;
        for (SourceUsageRecord.TableCapacityDetail table : project.getTables()) {
            long capacity = calculateTableCapacity(table, project);
            table.setCapacity(capacity);
            sum += capacity;
            SourceUsageRecord.CapacityStatus tableStatus = table.getStatus();
            if (tableStatus.compareTo(status) > 0) {
                status = tableStatus;
            }
        }
        project.setStatus(status);
        project.setCapacity(sum);
    }

    private void updateProjectUsageRatio(SourceUsageRecord.ProjectCapacityDetail project) {
        long projectCapacity = project.getCapacity();
        for (SourceUsageRecord.TableCapacityDetail table : project.getTables()) {
            double ratio = calculateRatio(table.getCapacity(), projectCapacity);
            table.setCapacityRatio(ratio);
        }
    }

    private void updateGlobalUsageRatio(SourceUsageRecord sourceUsageRecord) {
        long currentTotalCapacity = sourceUsageRecord.getCurrentCapacity();
        SourceUsageRecord.ProjectCapacityDetail[] projectCapacityDetails = sourceUsageRecord.getCapacityDetails();
        for (SourceUsageRecord.ProjectCapacityDetail capacityDetail : projectCapacityDetails) {
            double ratio = calculateRatio(capacityDetail.getCapacity(), currentTotalCapacity);
            capacityDetail.setCapacityRatio(ratio);
        }
    }

    private void getSumOfAllProjectSourceSizeBytes(SourceUsageRecord sourceUsageParams) {
        long sum = 0L;
        SourceUsageRecord.CapacityStatus status = SourceUsageRecord.CapacityStatus.OK;
        for (SourceUsageRecord.ProjectCapacityDetail project : sourceUsageParams.getCapacityDetails()) {
            sum += project.getCapacity();
            if (project.getStatus().compareTo(status) > 0) {
                status = project.getStatus();
            }
        }
        sourceUsageParams.setCapacityStatus(status);
        sourceUsageParams.setCurrentCapacity(sum);
    }

    private Map<TblColRef, Long> calculateColumnSourceInSegment(NDataSegment segment, IndexDependencyParser parser) {
        Map<TblColRef, Long> columnBytes = new HashMap<>();
        NDataModel model = segment.getModel();
        Set<TblColRef> usedColumns = new HashSet<>(getSegmentUsedColumns(segment, parser));

        Stream<Map.Entry<String, Long>> columnSourceBytesMapStream = segment.getColumnSourceBytes().entrySet().stream();

        if (model.isMultiPartitionModel()) {
            columnSourceBytesMapStream = segment.getMultiPartitions().stream()
                    .map(SegmentPartition::getColumnSourceBytes).map(Map::entrySet).flatMap(Collection::stream);
        }

        columnSourceBytesMapStream.forEach(sourceMap -> {
            String column = sourceMap.getKey();
            TblColRef columnColRef = model.findColumn(column);
            if (columnColRef != null && usedColumns.contains(columnColRef)) {
                TableRef tableRef = columnColRef.getTableRef();
                Long value = columnBytes.getOrDefault(columnColRef, 0L);
                if (model.isFactTable(tableRef)) {
                    columnBytes.put(columnColRef, sourceMap.getValue() + value);
                } else {
                    columnBytes.put(columnColRef, Math.max(sourceMap.getValue(), value));
                }
            }
        });

        usedColumns.forEach(column -> columnBytes.putIfAbsent(column, 0L));

        return columnBytes;
    }

    private Map<TblColRef, Long> calculateColumnSourceInDataflow(NDataflow dataflow) {
        Map<TblColRef, Long> dataflowColumnBytes = new HashMap<>();
        NDataModel model = dataflow.getModel();
        Segments<NDataSegment> segments = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        IndexDependencyParser parser = new IndexDependencyParser(model);

        for (NDataSegment segment : segments) {
            Map<TblColRef, Long> columnSourceInSegment = calculateColumnSourceInSegment(segment, parser);
            for (Map.Entry<TblColRef, Long> sourceMap : columnSourceInSegment.entrySet()) {
                TblColRef tblColRef = sourceMap.getKey();
                Long value = dataflowColumnBytes.getOrDefault(tblColRef, 0L);
                if (model.isFactTable(tblColRef.getTableRef())) {
                    dataflowColumnBytes.put(tblColRef, sourceMap.getValue() + value);
                } else {
                    dataflowColumnBytes.put(tblColRef, Math.max(sourceMap.getValue(), value));
                }
            }
        }
        return dataflowColumnBytes;
    }

    private void calculateTableInProject(NDataflow dataflow, SourceUsageRecord.ProjectCapacityDetail projectDetail) {
        NDataModel model = dataflow.getModel();
        if (dataflow.checkBrokenWithRelatedInfo()) {
            log.debug("Current model: {} is broken, skip calculate source usage", model);
            return;
        }

        // source usage is first captured by column, then sum up to table and project
        Map<TblColRef, Long> dataflowColumnsBytes = calculateColumnSourceInDataflow(dataflow);

        for (Map.Entry<TblColRef, Long> tblColRefLongEntry : dataflowColumnsBytes.entrySet()) {
            TblColRef column = tblColRefLongEntry.getKey();
            String tableName = column.getTableRef().getTableIdentity();
            String columnName = column.getCanonicalName();
            SourceUsageRecord.TableCapacityDetail tableDetail = projectDetail.getTableByName(tableName) == null
                    ? new SourceUsageRecord.TableCapacityDetail(tableName)
                    : projectDetail.getTableByName(tableName);
            SourceUsageRecord.ColumnCapacityDetail columnDetail = tableDetail.getColumnByName(columnName) == null
                    ? new SourceUsageRecord.ColumnCapacityDetail(columnName)
                    : tableDetail.getColumnByName(columnName);
            // simply return 0 for missing cols in flat table
            // as the cols in model definition may be different from segment flat table
            long sourceBytes = dataflowColumnsBytes.getOrDefault(column, 0L);
            columnDetail.setDataflowSourceBytes(dataflow.getId(), sourceBytes);
            tableDetail.updateColumn(columnDetail);
            checkTableKind(tableDetail, model);
            projectDetail.updateTable(tableDetail);
        }
    }

    public SourceUsageRecord refreshLatestSourceUsageRecord() {
        SourceUsageRecord usage = new SourceUsageRecord();
        // for each project, collect source usage
        for (ProjectInstance project : NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects()) {
            String projectName = project.getName();
            SourceUsageRecord.ProjectCapacityDetail projectDetail = new SourceUsageRecord.ProjectCapacityDetail(
                    projectName);

            // for each dataflow in project, collect table details

            for (NDataModel model : NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName)
                    .listUnderliningDataModels()) {
                try {
                    val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName)
                            .getDataflow(model.getId());
                    RealizationStatusEnum status = dataflow.getStatus();
                    if (status == RealizationStatusEnum.OFFLINE) {
                        log.debug("Model {} is {}, skip to calculate source usage in project {}", model.getAlias(),
                                status, project);
                        continue;
                    }
                    if (!isAllSegmentsEmpty(dataflow)) {
                        calculateTableInProject(dataflow, projectDetail);
                    }
                } catch (Exception e) {
                    log.error("Failed to get dataflow for {} in project: {}", model.getId(), projectName, e);
                }
            }
            updateProjectSourceUsage(projectDetail);
            updateProjectUsageRatio(projectDetail);
            if (projectDetail.getCapacity() > 0) {
                usage.appendProject(projectDetail);
            }
            SourceUsageRecord.CapacityStatus defaultStatus = SourceUsageRecord.CapacityStatus.OK;
            SourceUsageRecord.CapacityStatus projectStatus = projectDetail.getStatus();
            if (projectStatus.compareTo(defaultStatus) > 0) {
                usage.setCapacityStatus(projectStatus);
            }
        }

        getSumOfAllProjectSourceSizeBytes(usage);

        updateGlobalUsageRatio(usage);
        usage.setCheckTime(System.currentTimeMillis());

        String capacity = System.getProperty(Constants.KE_LICENSE_VOLUME);
        if (Constants.UNLIMITED.equals(capacity)) {
            usage.setLicenseCapacity(-1L);
        } else if (!StringUtils.isEmpty(capacity)) {
            try {
                long licenseCapacity = Long.parseLong(capacity);
                usage.setLicenseCapacity(licenseCapacity);
                SourceUsageRecord.CapacityStatus currentStatus = usage.getCapacityStatus();
                if (isNotOk(currentStatus)) {
                    log.debug("Current capacity status: {} is not ok, will skip overcapacity check", currentStatus);
                } else if (licenseCapacity < usage.getCurrentCapacity()) {
                    usage.setCapacityStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
                }
            } catch (NumberFormatException e) {
                log.error("ke.license.volume occurred java.lang.NumberFormatException: For input string:" + capacity,
                        e);
            }
        }
        return usage;
    }

    private boolean isAllSegmentsEmpty(NDataflow dataflow) {
        if (dataflow == null) {
            return true;
        }
        Segments<NDataSegment> segments = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        if (segments.isEmpty()) {
            return true;
        }
        boolean isAllEmpty = true;
        for (NDataSegment segment : segments) {
            if (segment.getSourceCount() > 0) {
                isAllEmpty = false;
                break;
            }
        }
        return isAllEmpty;
    }

    @VisibleForTesting
    public boolean isAllSegmentsEmptyFromDataflow(NDataflow dataflow) {
        return isAllSegmentsEmpty(dataflow);
    }

    private boolean isNotOk(SourceUsageRecord.CapacityStatus status) {
        return SourceUsageRecord.CapacityStatus.TENTATIVE == status || SourceUsageRecord.CapacityStatus.ERROR == status;
    }

    public double calculateRatio(long amount, long totalAmount) {
        if (amount > 0d) {
            // Keep two decimals
            return (Math.round(((double) amount) / totalAmount * 100d)) / 100d;
        }
        return 0d;
    }

    // public for ut
    public static Set<TblColRef> getSegmentUsedColumns(NDataSegment segment, IndexDependencyParser parser) {
        NDataModel dataModel = segment.getModel();

        return segment.getLayoutsMap().values().stream().map(layout -> layout.getLayout().getColOrder())
                .flatMap(Collection::stream).distinct().map(colOrderId -> {
                    if (colOrderId < NDataModel.MEASURE_ID_BASE) {
                        return Optional.ofNullable(dataModel.getEffectiveCols().get(colOrderId)).map(Arrays::asList)
                                .orElseGet(ArrayList::new);
                    } else {
                        return Optional
                                .ofNullable(dataModel.getEffectiveMeasures().get(colOrderId).getFunction().getColRefs())
                                .orElseGet(ArrayList::new);
                    }
                }).flatMap(Collection::stream).filter(Objects::nonNull).map(tblColRef -> {
                    if (tblColRef.getColumnDesc().isComputedColumn()
                            && KapConfig.getInstanceFromEnv().isSourceUsageUnwrapComputedColumn()) {
                        try {
                            return parser.unwrapComputeColumn(tblColRef.getExpressionInSourceDB());
                        } catch (Exception e) {
                            log.warn("UnWrap computed column {} in project {} model {} exception",
                                    tblColRef.getExpressionInSourceDB(), dataModel.getProject(), dataModel.getAlias(),
                                    e);
                        }
                        return null;
                    }
                    return Collections.singletonList(tblColRef);
                }).filter(Objects::nonNull).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public void updateSourceUsage() {
        if (EpochManager.getInstance().checkEpochOwner(EpochManager.GLOBAL)) {
            SourceUsageRecord sourceUsageRecord = this.refreshLatestSourceUsageRecord();
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv()).updateSourceUsage(sourceUsageRecord);
                return 0;
            }, EpochManager.GLOBAL);
        }
    }

}
