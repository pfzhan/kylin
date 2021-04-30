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
package io.kyligence.kap.metadata.sourceusage;

import static io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.CapacityStatus.OVERCAPACITY;
import static org.apache.kylin.common.exception.CommonErrorCode.LICENSE_OVER_CAPACITY;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.MailHelper;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.constant.Constants;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.CapacityStatus;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.ColumnCapacityDetail;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.ProjectCapacityDetail;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.TableCapacityDetail;
import lombok.val;

public class SourceUsageManager {

    private static final Logger logger = LoggerFactory.getLogger(SourceUsageManager.class);

    public static SourceUsageManager getInstance(KylinConfig config) {
        return config.getManager(SourceUsageManager.class);
    }

    // called by reflection
    static SourceUsageManager newInstance(KylinConfig config) {
        return new SourceUsageManager(config);
    }

    // ============================================================================

    final KylinConfig config;

    private final CachedCrudAssist<SourceUsageRecord> crud;

    private SourceUsageManager(KylinConfig config) {
        this.config = config;
        this.crud = new CachedCrudAssist<SourceUsageRecord>(getStore(), ResourceStore.HISTORY_SOURCE_USAGE,
                SourceUsageRecord.class) {
            @Override
            protected SourceUsageRecord initEntityAfterReload(SourceUsageRecord entity, String resourceName) {
                entity.setResPath(concatResourcePath(resourceName));
                return entity;
            }
        };
    }

    public SourceUsageRecord getSourceUsageRecord(String resourceName) {
        return this.crud.listAll().stream()
                .filter(usage -> usage.getResourcePath().equalsIgnoreCase(concatResourcePath(resourceName))).findAny()
                .orElse(null);
    }

    public interface SourceUsageRecordUpdater {
        void modify(SourceUsageRecord record);
    }

    public SourceUsageRecord copy(SourceUsageRecord df) {
        return crud.copyBySerialization(df);
    }

    public SourceUsageRecord createSourceUsageRecord(String resourceName, SourceUsageRecord record) {
        record.setResPath(concatResourcePath(resourceName));
        return crud.save(record);
    }

    public SourceUsageRecord updateSourceUsageRecord(String resourceName, SourceUsageRecordUpdater updater) {
        SourceUsageRecord record = getSourceUsageRecord(resourceName);
        if (record == null) {
            record = new SourceUsageRecord();
        }
        SourceUsageRecord copy = copy(record);
        updater.modify(copy);
        return crud.save(copy);
    }

    public static String concatResourcePath(String resourceName) {
        return ResourceStore.HISTORY_SOURCE_USAGE + "/" + resourceName + MetadataConstants.FILE_SURFIX;
    }

    public Map<String, Long> calcAvgColumnSourceBytes(NDataSegment segment) {
        Map<String, Long> columnSourceBytes = Maps.newHashMap();
        Set<TblColRef> usedColumns;
        try {
            usedColumns = new NCubeJoinedFlatTableDesc(segment).getUsedColumns();
        } catch (Exception e) {
            return columnSourceBytes;
        }
        long inputRecordsSize = segment.getSourceBytesSize();
        if (inputRecordsSize == -1) {
            logger.debug("Source bytes size for segment: {} is -1", segment);
            return columnSourceBytes;
        }
        if (usedColumns.isEmpty()) {
            logger.debug("No effective columns found in segment: {}", segment);
            return columnSourceBytes;
        }
        List<NDataModel.NamedColumn> allColumns = Lists
                .newArrayList(segment.getModel().getEffectiveNamedColumns().values());
        int columnSize = allColumns.isEmpty() ? usedColumns.size() : allColumns.size();
        // all named columns as denominator, since inputRecordsSize includes all cols on table
        long perColumnSize = inputRecordsSize / columnSize;
        for (TblColRef tblColRef : usedColumns) {
            columnSourceBytes.put(tblColRef.getCanonicalName(), perColumnSize);
        }
        return columnSourceBytes;
    }

    @VisibleForTesting
    public Map<String, Long> sumDataflowColumnSourceMap(NDataflow dataflow) {
        Map<String, Long> dataflowSourceMap = new HashMap<>();
        Segments<NDataSegment> segments = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        List<NDataSegment> oldSegments = Lists.newArrayList();
        for (NDataSegment segment : segments) {
            Map<String, Long> columnSourceBytesMap = segment.getColumnSourceBytes();
            if (MapUtils.isEmpty(columnSourceBytesMap)) {
                oldSegments.add(segment);
            } else {
                for (Map.Entry<String, Long> sourceMap : columnSourceBytesMap.entrySet()) {
                    String column = sourceMap.getKey();
                    long value = dataflowSourceMap.getOrDefault(column, 0L);
                    dataflowSourceMap.put(column, sourceMap.getValue() + value);
                }
            }
        }
        if (!oldSegments.isEmpty()) {
            List<NDataSegment> evaluations = isPartitioned(dataflow) ? oldSegments // all old segments
                    : Lists.newArrayList(oldSegments.get(oldSegments.size() - 1)); // last old segment
            for (NDataSegment segment : evaluations) {
                Map<String, Long> estimateSourceMap = calcAvgColumnSourceBytes(segment);
                for (Map.Entry<String, Long> sourceMap : estimateSourceMap.entrySet()) {
                    String column = sourceMap.getKey();
                    long value = dataflowSourceMap.getOrDefault(column, 0L);
                    dataflowSourceMap.put(column, sourceMap.getValue() + value);
                }
            }
        }
        return dataflowSourceMap;
    }

    private boolean isPartitioned(NDataflow dataflow) {
        val partDesc = dataflow.getModel().getPartitionDesc();
        if (Objects.isNull(partDesc)) {
            return false;
        }
        val colRef = partDesc.getPartitionDateColumnRef();
        if (Objects.isNull(colRef)) {
            return false;
        }
        return colRef.getColumnDesc().isPartitioned();
    }

    private long calculateTableInputBytes(TableCapacityDetail tableDetail) {
        long sumBytes = 0;
        for (ColumnCapacityDetail column : tableDetail.getColumns()) {
            sumBytes += column.getMaxSourceBytes();
        }
        return sumBytes;
    }

    private long getLookupTableSource(TableCapacityDetail table, ProjectCapacityDetail project) {
        String projectName = project.getName();
        String tableName = table.getName();
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, projectName);
        TableDesc tableDesc = tableManager.getTableDesc(tableName);
        if (tableManager.existsSnapshotTableByName(tableName)) {
            TableExtDesc tableExtDesc = tableManager.getOrCreateTableExt(tableDesc);
            long originalSize = tableExtDesc.getOriginalSize();
            if (originalSize == -1) {
                logger.warn("Original size of table:{} is -1, set table status to TENTATIVE", tableName);
                table.setStatus(CapacityStatus.TENTATIVE);
            }
            return originalSize;
        } else {
            return getLookupTableSourceByScale(table, projectName);
        }
    }

    // for calculating lookup table without snapshot source usage
    private long getLookupTableSourceByScale(TableCapacityDetail table, String projectName) {
        String tableName = table.getName();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, projectName);
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, projectName);
        TableExtDesc tableExtDesc = tableManager.getOrCreateTableExt(tableName);
        long tableBytes = 0L;
        try {
            double rowBytes = 0;
            for (ColumnCapacityDetail column : table.getColumns()) {
                long recordCount = 0;
                long columnBytes = 0;
                for (String dataflow : column.getSourceBytesMap().keySet()) {
                    val sourceCount = dataflowManager.getDataflow(dataflow).getLastSegment().getSourceCount();
                    if (sourceCount == -1)
                        continue;

                    recordCount += sourceCount;
                    columnBytes += column.getDataflowSourceBytes(dataflow);
                }

                if (recordCount <= 0)
                    continue;

                rowBytes += (double) columnBytes / (double) recordCount;
            }
            long tableTotalRows = tableExtDesc.getTotalRows();
            if (tableTotalRows == 0L) {
                logger.debug("Total rows for table: {} is zero.", tableName);
            }
            tableBytes = (long) (rowBytes * tableTotalRows);
        } catch (Exception e) {
            logger.error("Failed to calculate lookup table: {} source usage.", tableName, e);
            table.setStatus(SourceUsageRecord.CapacityStatus.ERROR);
            return 0;
        }
        return tableBytes;
    }

    private void checkTableKind(TableCapacityDetail tableDetail, NDataModel model) {
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

    private long calculateTableSourceBytes(TableCapacityDetail table, ProjectCapacityDetail project) {
        long sourceBytes;
        if (SourceUsageRecord.TableKind.FACT == table.getTableKind()) {
            sourceBytes = Long.MAX_VALUE - 1;
        } else {
            sourceBytes = getLookupTableSource(table, project);
        }
        return sourceBytes;
    }

    private void updateProjectSourceUsage(ProjectCapacityDetail project) {
        long sum = 0L;
        CapacityStatus status = SourceUsageRecord.CapacityStatus.OK;
        for (TableCapacityDetail table : project.getTables()) {
            long sourceCapacity = calculateTableSourceBytes(table, project);
            long inputCapacity = calculateTableInputBytes(table);
            long capacity = Math.min(sourceCapacity, inputCapacity);
            table.setCapacity(capacity);
            sum += capacity;
            CapacityStatus tableStatus = table.getStatus();
            if (tableStatus.compareTo(status) > 0) {
                status = tableStatus;
            }
        }
        project.setStatus(status);
        project.setCapacity(sum);
    }

    private void updateProjectUsageRatio(ProjectCapacityDetail project) {
        long projectCapacity = project.getCapacity();
        for (TableCapacityDetail table : project.getTables()) {
            double ratio = calculateRatio(table.getCapacity(), projectCapacity);
            table.setCapacityRatio(ratio);
        }
    }

    private void updateGlobalUsageRatio(SourceUsageRecord sourceUsageRecord) {
        long currentTotalCapacity = sourceUsageRecord.getCurrentCapacity();
        ProjectCapacityDetail[] projectCapacityDetails = sourceUsageRecord.getCapacityDetails();
        for (ProjectCapacityDetail capacityDetail : projectCapacityDetails) {
            double ratio = calculateRatio(capacityDetail.getCapacity(), currentTotalCapacity);
            capacityDetail.setCapacityRatio(ratio);
        }
    }

    private void getSumOfAllProjectSourceSizeBytes(SourceUsageRecord sourceUsageParams) {
        long sum = 0L;
        CapacityStatus status = CapacityStatus.OK;
        for (ProjectCapacityDetail project : sourceUsageParams.getCapacityDetails()) {
            sum += project.getCapacity();
            if (project.getStatus().compareTo(status) > 0) {
                status = project.getStatus();
            }
        }
        sourceUsageParams.setCapacityStatus(status);
        sourceUsageParams.setCurrentCapacity(sum);
    }

    private void calculateTableInProject(NDataflow dataflow, ProjectCapacityDetail projectDetail) {
        NDataModel model = dataflow.getModel();
        if (dataflow.checkBrokenWithRelatedInfo()) {
            logger.debug("Current model: {} is broken, skip calculate source usage", model);
            return;
        }
        // source usage is first captured by column, then sum up to table and project
        Map<String, Long> dataflowColumnsBytes = sumDataflowColumnSourceMap(dataflow);

        NDataSegment dataSegment = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING).get(0);
        Set<TblColRef> allColumns = Sets.newHashSet();
        try {
            allColumns = new NCubeJoinedFlatTableDesc(dataSegment).getUsedColumns();
        } catch (Exception e) {
            logger.error("Failed to get all columns' TblColRef for segment: {}", dataSegment, e);
            projectDetail.setStatus(CapacityStatus.ERROR);
        }
        for (TblColRef column : allColumns) {
            String tableName = column.getTableRef().getTableIdentity();
            String columnName = column.getCanonicalName();
            TableCapacityDetail tableDetail = projectDetail.getTableByName(tableName) == null
                    ? new TableCapacityDetail(tableName)
                    : projectDetail.getTableByName(tableName);
            ColumnCapacityDetail columnDetail = tableDetail.getColumnByName(columnName) == null
                    ? new ColumnCapacityDetail(columnName)
                    : tableDetail.getColumnByName(columnName);
            // simply return 0 for missing cols in flat table
            // as the cols in model definition may be different from segment flat table
            long sourceBytes = dataflowColumnsBytes.getOrDefault(columnName, 0L);
            columnDetail.setDataflowSourceBytes(dataflow.getId(), sourceBytes);
            tableDetail.updateColumn(columnDetail);
            checkTableKind(tableDetail, model);
            projectDetail.updateTable(tableDetail);
        }
    }

    /**
     * Update source usage based on latest metadata.
     *
     * This is a fast operation that DOES NOT read from external data source.
     */
    public SourceUsageRecord updateSourceUsage() {
        if (!KapConfig.wrap(config).isRecordSourceUsage())
            return null;

        logger.info("Updating source usage..");
        try {
            SourceUsageRecord sourceUsageRecord = refreshLatestSourceUsageRecord();
            return updateSourceUsage(sourceUsageRecord);
        } catch (Exception ex) {
            // swallow exception, source usage problem is not as critical as daily operations
            logger.error("Failed to update source usage", ex);
            return null;
        }
    }

    public SourceUsageRecord updateSourceUsage(SourceUsageRecord sourceUsageRecord) {
        createOrUpdate(sourceUsageRecord);
        return sourceUsageRecord;
    }

    public SourceUsageRecord refreshLatestSourceUsageRecord() {
        SourceUsageRecord usage = new SourceUsageRecord();
        // for each project, collect source usage
        for (ProjectInstance project : NProjectManager.getInstance(config).listAllProjects()) {
            String projectName = project.getName();
            ProjectCapacityDetail projectDetail = new ProjectCapacityDetail(projectName);

            // for each dataflow in project, collect table details

            for (NDataModel model : NDataflowManager.getInstance(config, projectName).listUnderliningDataModels()) {
                try {
                    val dataflow = NDataflowManager.getInstance(config, projectName).getDataflow(model.getId());
                    if (!isAllSegmentsEmpty(dataflow)) {
                        calculateTableInProject(dataflow, projectDetail);
                    }
                } catch (Exception e) {
                    logger.error("Failed to get dataflow for {} in project: {}", model.getId(), projectName, e);
                }
            }
            updateProjectSourceUsage(projectDetail);
            updateProjectUsageRatio(projectDetail);
            if (projectDetail.getCapacity() > 0) {
                usage.appendProject(projectDetail);
            }
            CapacityStatus defaultStatus = CapacityStatus.OK;
            CapacityStatus projectStatus = projectDetail.getStatus();
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
                CapacityStatus currentStatus = usage.getCapacityStatus();
                if (isNotOk(currentStatus)) {
                    logger.debug("Current capacity status: {} is not ok, will skip overcapacity check", currentStatus);
                } else if (licenseCapacity < usage.getCurrentCapacity()) {
                    usage.setCapacityStatus(OVERCAPACITY);
                }
            } catch (NumberFormatException e) {
                logger.error("ke.license.volume occurred java.lang.NumberFormatException: For input string:" + capacity,
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
            if (segment.getSourceBytesSize() > 0) {
                isAllEmpty = false;
                break;
            }
        }
        return isAllEmpty;
    }

    private void createOrUpdate(SourceUsageRecord usageRecord) {
        try {
            String resourceName = usageRecord.resourceName();
            if (resourceName == null) {
                resourceName = DateFormat.formatToCompactDateStr(System.currentTimeMillis());
            }
            SourceUsageRecord record = getSourceUsageRecord(resourceName);
            if (record == null) {
                record = createSourceUsageRecord(resourceName, usageRecord);
            } else {
                record = updateSourceUsageRecord(resourceName, copyForWrite -> {
                    copyForWrite.setLicenseCapacity(usageRecord.getLicenseCapacity());
                    copyForWrite.setCapacityDetails(usageRecord.getCapacityDetails());
                    copyForWrite.setCapacityStatus(usageRecord.getCapacityStatus());
                    copyForWrite.setCheckTime(usageRecord.getCheckTime());
                    copyForWrite.setCurrentCapacity(usageRecord.getCurrentCapacity());
                    if (!isOverCapacityThreshold(copyForWrite) && !copyForWrite.isCapacityNotification()) {
                        copyForWrite.setCapacityNotification(true);
                        logger.info("Capacity usage is less than threshold, enable notification");
                    } else if (copyForWrite.isCapacityNotification() && config.isOverCapacityNotificationEnabled()
                            && isOverCapacityThreshold(copyForWrite)) {
                        if (MailHelper.notifyUserForOverCapacity(copyForWrite.getLicenseCapacity(),
                                copyForWrite.getCurrentCapacity())) {
                            copyForWrite.setCapacityNotification(false);
                            logger.info("Capacity usage is more than threshold, disable notification");
                        } else {
                            logger.info("Send mail for Over Capacity failed.");
                        }
                    }
                });
            }
            usageRecord.setCapacityNotification(record.isCapacityNotification());
        } catch (Exception e) {
            logger.error("Failed to update source usage record.", e);
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    /**
     * Read external data source to refresh lookup table row count for a table.
     */
    public void refreshLookupTableRowCount(final TableDesc tableDesc, String project) {
        // TODO no this case currently, because no need to calculate fact table and lookup table without snapshot
    }

    public SourceUsageRecord getLatestRecord() {
        int oneDay = 24;

        // try to reduce the scan range
        SourceUsageRecord r;
        r = getLatestRecord(oneDay);
        if (r == null)
            r = getLatestRecord(oneDay * 7);
        if (r == null)
            r = getLatestRecord(oneDay * 31);
        if (r == null)
            return getLatestRecord(oneDay * getThresholdByDayFromOrigin());

        return r;
    }

    public SourceUsageRecord getLatestRecord(int hoursAgo) {
        List<SourceUsageRecord> recordList = getLatestRecordByHours(hoursAgo);
        if (CollectionUtils.isEmpty(recordList)) {
            return null;
        }
        return Collections.max(recordList, (o1, o2) -> {
            long comp = o1.getLastModified() - o2.getLastModified();
            if (comp == 0) {
                return 0;
            }
            return comp < 0 ? -1 : 1;
        });
    }

    public List<SourceUsageRecord> getLatestRecordByMs(long msAgo) {
        long from = System.currentTimeMillis() - msAgo;
        return this.crud.listAll().stream().filter(usage -> usage.getCreateTime() >= from).collect(Collectors.toList());
    }

    public List<SourceUsageRecord> getAllRecords() {
        return this.crud.listAll();
    }

    public void delSourceUsage(String resourceName) {
        this.crud.delete(resourceName);
    }

    public List<SourceUsageRecord> getLatestRecordByHours(int hoursAgo) {
        return getLatestRecordByMs(hoursAgo * 3600L * 1000L);
    }

    public List<SourceUsageRecord> getLatestRecordByDays(int daysAgo) {
        return getLatestRecordByHours(daysAgo * 24);
    }

    private int getThresholdByDayFromOrigin() {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd",
                    Locale.getDefault(Locale.Category.FORMAT));
            Date date = simpleDateFormat.parse("1970-01-01");
            return (int) ((System.currentTimeMillis() - date.getTime()) / (1000 * 60 * 60 * 24));
        } catch (ParseException e) {
            logger.error("parser date error", e);
            return 365 * 10;
        }
    }

    // return all records in last one month
    public List<SourceUsageRecord> getLastMonthRecords() {
        return getLatestRecordByDays(30);
    }

    public List<SourceUsageRecord> getLastQuarterRecords() {
        return getLatestRecordByDays(90);
    }

    public List<SourceUsageRecord> getLastYearRecords() {
        return getLatestRecordByDays(365);
    }

    private boolean isNotOk(SourceUsageRecord.CapacityStatus status) {
        return SourceUsageRecord.CapacityStatus.TENTATIVE == status || SourceUsageRecord.CapacityStatus.ERROR == status;
    }

    private void setNodeInfo(LicenseInfo info) {
        //node part
        List<String> servers = config.getAllServers();
        int currentNodes = servers.size();
        info.setCurrentNode(currentNodes);

        String licenseNodes = System.getProperty(Constants.KE_LICENSE_NODES);
        if (Constants.UNLIMITED.equals(licenseNodes)) {
            info.setNode(-1);
        } else if (!StringUtils.isEmpty(licenseNodes)) {
            try {
                int maximumNodeNums = Integer.parseInt(licenseNodes);
                info.setNode(maximumNodeNums);

                if (maximumNodeNums < currentNodes) {
                    info.setNodeStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
                }
            } catch (NumberFormatException e) {
                logger.error("Illegal value of config ke.license.nodes", e);
            }
        }
    }

    private void setSourceUsageInfo(LicenseInfo info, String project) {
        //capacity part
        SourceUsageRecord latestHistory = getLatestRecord();

        if (latestHistory != null) {
            info.setTime(latestHistory.getCheckTime());

            if (project == null) {
                info.setCurrentCapacity(latestHistory.getCurrentCapacity());
                info.setCapacity(latestHistory.getLicenseCapacity());
                info.setCapacityStatus(latestHistory.getCapacityStatus());
            } else {
                ProjectCapacityDetail projectCapacity = latestHistory.getProjectCapacity(project);

                if (projectCapacity != null) {
                    info.setCurrentCapacity(projectCapacity.getCapacity());
                    info.setCapacity(projectCapacity.getLicenseCapacity());
                    info.setCapacityStatus(projectCapacity.getStatus());
                }
            }

            if (isNotOk(latestHistory.getCapacityStatus())) {
                List<SourceUsageRecord> recentHistories = SourceUsageManager.getInstance(config).getLastMonthRecords();

                info.setFirstErrorTime(latestHistory.getCheckTime());
                for (int i = recentHistories.size() - 1; i >= 0; i--) {
                    SourceUsageRecord historyRecord = recentHistories.get(i);
                    if (isNotOk(historyRecord.getCapacityStatus())) {
                        info.setFirstErrorTime(historyRecord.getCheckTime());
                    } else {
                        break;
                    }
                }
            }
        } else {
            logger.warn("Latest history of source usage record is null.");
        }
    }

    private LicenseInfo getLicenseInfo(String project) {
        LicenseInfo info = new LicenseInfo();

        setNodeInfo(info);
        setSourceUsageInfo(info, project);

        long firstErrorTime = info.getFirstErrorTime();
        if (firstErrorTime != 0L) {
            long dayThreshold = (System.currentTimeMillis() - firstErrorTime) / (1000 * 60 * 60 * 24);
            if (dayThreshold >= 30) {
                logger.warn("Failed to fetch data volume usage for over {} days", dayThreshold);
            }
        }
        return info;
    }

    public void checkIsOverCapacity(String project) {
        if (config.isUTEnv()) {
            return;
        }

        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isRecordSourceUsage()) {
            logger.info("Skip check over capacity.");
            return;
        }

        boolean checkProject = false;

        if (project != null) {
            KylinConfig kylinConfig = NProjectManager.getInstance(config).getProject(project).getConfig();

            // have project usage capacity config
            if (kylinConfig.getSourceUsageQuota() != -1) {
                checkProject = true;
            }
        }

        LicenseInfo info = getLicenseInfo(checkProject ? project : null);
        CapacityStatus capacityStatus = info.getCapacityStatus();
        if (isNotOk(capacityStatus)) {
            logger.warn("Capacity status is not ok: {}, will not block build job", capacityStatus);
            return;
        }
        if (checkProject) {
            if (info.getCapacityStatus() == OVERCAPACITY && info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getLICENSE_PROJECT_SOURCE_NODES_OVER_CAPACITY(),
                                info.getCurrentCapacity(), info.getCapacity(), info.getCurrentNode(), info.getNode()));
            } else if (info.getCapacityStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getLICENSE_PROJECT_SOURCE_OVER_CAPACITY(),
                                info.getCurrentCapacity(), info.getCapacity()));
            } else if (info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getLICENSE_NODES_OVER_CAPACITY(), info.getCurrentNode(), info.getNode()));
            }
            logger.info("Current capacity status of project: {} is ok", project);
        } else {
            if (info.getCapacityStatus() == OVERCAPACITY && info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getLICENSE_SOURCE_NODES_OVER_CAPACITY(),
                                info.getCurrentCapacity(), info.getCapacity(), info.getCurrentNode(), info.getNode()));
            } else if (info.getCapacityStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getLICENSE_SOURCE_OVER_CAPACITY(),
                                info.getCurrentCapacity(), info.getCapacity()));
            } else if (info.getNodeStatus() == OVERCAPACITY) {
                throw new KylinException(LICENSE_OVER_CAPACITY, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getLICENSE_NODES_OVER_CAPACITY(), info.getCurrentNode(), info.getNode()));
            }
            logger.info("Current capacity status is ok");
        }
    }

    public boolean isOverCapacityThreshold(SourceUsageRecord sourceUsageRecord) {
        if (Constants.UNLIMITED.equals(System.getProperty(Constants.KE_LICENSE_VOLUME))) {
            logger.info("Current license has unlimited volume.");
            return false;
        }
        if (sourceUsageRecord == null) {
            logger.debug("Source usage record is null, ignore...");
            return false;
        }
        long currentCapacity = sourceUsageRecord.getCurrentCapacity();
        long totalCapacity = sourceUsageRecord.getLicenseCapacity();
        logger.info("Current capacity is: {}, total capacity is: {}", currentCapacity, totalCapacity);
        return currentCapacity > totalCapacity * config.getOverCapacityThreshold();
    }

    public <T> T licenseCheckWrap(String project, Callback<T> f) {
        checkIsOverCapacity(project);

        return f.process();
    }

    public interface Callback<T> {
        T process();
    }

    public double calculateRatio(long amount, long totalAmount) {
        if (amount > 0d) {
            // Keep two decimals
            return ((double) Math.round(((double) amount) / totalAmount * 100d)) / 100d;
        }
        return 0d;
    }
}
