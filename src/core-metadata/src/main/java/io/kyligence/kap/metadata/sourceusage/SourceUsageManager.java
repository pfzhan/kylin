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

import com.google.common.collect.Maps;
import io.kyligence.kap.common.license.Constants;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.CapacityStatus;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.ColumnCapacityDetail;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.ProjectCapacityDetail;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord.TableCapacityDetail;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SourceUsageManager {

    private static final Logger logger = LoggerFactory.getLogger(SourceUsageManager.class);

    private static final Serializer<SourceUsageRecord> SOURCE_USAGE_SERIALIZER = new JsonSerializer<>(SourceUsageRecord.class);
    public static final String GLOBAL = "_global";

    public static SourceUsageManager getInstance(KylinConfig config) {
        return config.getManager(SourceUsageManager.class);
    }

    // called by reflection
    static SourceUsageManager newInstance(KylinConfig config) {
        return new SourceUsageManager(config);
    }

    // ============================================================================

    final KylinConfig config;

    public SourceUsageManager(KylinConfig config) {
        this.config = config;
    }

    private Map<String, Long> calcAvgColumnSourceBytes(NDataSegment segment) {
        Map<String, Long> columnSourceBytes = Maps.newHashMap();
        Set<TblColRef> allColumns = new HashSet<>(new NCubeJoinedFlatTableDesc(segment).getAllColumns());
        long inputRecordsSize = segment.getSourceBytesSize();
        if (allColumns.isEmpty()) {
            return columnSourceBytes;
        }
        long perColumnSize = inputRecordsSize / allColumns.size();
        for (TblColRef tblColRef : allColumns) {
            columnSourceBytes.put(tblColRef.getIdentity(), perColumnSize);
        }
        segment.setColumnSourceBytes(columnSourceBytes);
        return columnSourceBytes;
    }

    private Map<String, Long> sumDataflowColumnSourceMap(NDataflow dataflow) {
        Map<String, Long> dataflowSourceMap = new HashMap<>();
        for (NDataSegment segment : dataflow.getSegments(SegmentStatusEnum.READY)) {
            Map<String, Long> segmentSourceMap = segment.getColumnSourceBytes().size() > 0
                    ? segment.getColumnSourceBytes()
                    : calcAvgColumnSourceBytes(segment);
            for (Map.Entry<String, Long> sourceMap : segmentSourceMap.entrySet()) {
                String column = sourceMap.getKey();
                Long value = dataflowSourceMap.getOrDefault(column, 0L);
                dataflowSourceMap.put(column, sourceMap.getValue() + value);
            }
        }
        return dataflowSourceMap;
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
            long originalSize = tableDesc.getOriginalSize();
            if (originalSize != 0) {
                return originalSize;
            }
            return getLookupTableSourceByScale(table, projectName);
        } else {
            return 0;
        }
    }

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
                    recordCount += dataflowManager.getDataflow(dataflow).getStorageBytesSize();
                    columnBytes += column.getDataflowSourceBytes(dataflow);
                }

                if (recordCount <= 0)
                    continue;

                rowBytes += (double) columnBytes / (double) recordCount;
            }
            tableBytes = (long) (rowBytes * tableExtDesc.getTotalRows());
            if (!TableExtDesc.RowCountStatus.OK.equals(tableExtDesc.getRowCountStatus())) {
                table.setStatus(SourceUsageRecord.CapacityStatus.TENTATIVE);
            }
        } catch (Exception e) {
            logger.error("Fail to calculate lookup table.", e);
            table.setStatus(SourceUsageRecord.CapacityStatus.ERROR);
            return 0;
        }
        return tableBytes;
    }

    private void checkTableKind(TableCapacityDetail tableDetail, NDataModel model) {
        String tableName = tableDetail.getName();
        if (tableName.equals(model.getRootFactTableName())
                && !SourceUsageRecord.TableKind.FACT.equals(tableDetail.getTableKind())) {
            tableDetail.setTableKind(SourceUsageRecord.TableKind.FACT);
        } else if (model.isLookupTable(tableName)
                && !SourceUsageRecord.TableKind.FACT.equals(tableDetail.getTableKind())) {
            tableDetail.setTableKind(SourceUsageRecord.TableKind.WITHSNAP);
        } else if (model.isFactTable(tableName) && !SourceUsageRecord.TableKind.FACT.equals(tableDetail.getTableKind())
                && !SourceUsageRecord.TableKind.WITHSNAP.equals(tableDetail.getTableKind())) {
            tableDetail.setTableKind(SourceUsageRecord.TableKind.WITHOUTSNAP);
        }
    }

    private long calculateTableSourceBytes(TableCapacityDetail table, ProjectCapacityDetail project) {
        long sourceBytes = 0L;
        if (table.getTableKind().equals(SourceUsageRecord.TableKind.FACT)) {
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
            if (table.getStatus().compareTo(status) > 0) {
                status = table.getStatus();
            }
        }
        project.setStatus(status);
        project.setCapacity(sum);
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

        // source usage is first captured by column, then sum up to table and project
        Map<String, Long> dataflowColumnsBytes = sumDataflowColumnSourceMap(dataflow);

        NDataSegment dataSegment = dataflow.getSegments(SegmentStatusEnum.READY).get(0);
        List<TblColRef> allColumns = new NCubeJoinedFlatTableDesc(dataSegment).getAllColumns();
        for (TblColRef column : allColumns) {
            String tableName = column.getTableRef().getTableIdentity();
            TableCapacityDetail tableDetail = projectDetail.getTableByName(tableName) == null
                    ? new TableCapacityDetail(tableName)
                    : projectDetail.getTableByName(tableName);
            ColumnCapacityDetail columnDetail = tableDetail.getColumnByName(column.getIdentity()) == null
                    ? new ColumnCapacityDetail(column.getIdentity())
                    : tableDetail.getColumnByName(column.getIdentity());
            long sourceBytes = dataflowColumnsBytes.get(column.getIdentity());
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
            SourceUsageRecord usage = updateSourceUsageInner();
            logger.info("Updating source usage done: {}", usage);
            return usage;
        } catch (Exception ex) {
            // swallow exception, source usage problem is not as critical as daily operations
            logger.error("Failed to update source usage", ex);
            return null;
        }
    }

    private SourceUsageRecord updateSourceUsageInner() {
        SourceUsageRecord usage = new SourceUsageRecord();
        long start = System.currentTimeMillis();

        // for each project, collect source usage
        for (ProjectInstance project : NProjectManager.getInstance(config).listAllProjects()) {
            ProjectCapacityDetail projectDetail = new ProjectCapacityDetail(project.getName());

            // for each dataflow in project, collect table details
            for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
                NDataflow dataflow = NDataflowManager.getInstance(config, project.getName()).getDataflow(projectDataModel.getRealization());
                if (dataflow != null && !dataflow.getSegments(SegmentStatusEnum.READY).isEmpty()) {
                    calculateTableInProject(dataflow, projectDetail);
                }
            }
            updateProjectSourceUsage(projectDetail);
            if (projectDetail.getCapacity() > 0) {
                usage.appendProject(projectDetail);
            }
        }

        getSumOfAllProjectSourceSizeBytes(usage);
        usage.setCheckTime(System.currentTimeMillis());

        String capacity = System.getProperty(Constants.KE_LICENSE_VOLUME);
        if (!StringUtils.isEmpty(capacity) && !"Unlimited".equals(capacity)) {
            try {
                double licenseCapacity = tbToByte(Double.parseDouble(capacity));
                usage.setLicenseCapacity(licenseCapacity);
                if (licenseCapacity < usage.getCurrentCapacity()) {
                    usage.setCapacityStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
                }
            } catch (NumberFormatException e) {
                logger.error("ke.license.volume occurred java.lang.NumberFormatException: For input string:" + capacity,
                        e);
            }
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(ResourceStore.HISTORY_SOURCE_USAGE, usage, SOURCE_USAGE_SERIALIZER);
            return 0;
        }, GLOBAL, 1);
        logger.info("Updating source usage done: {}", usage);
        return usage;
    }

    private double tbToByte(double tb) {
        return tb * 1024 * 1024 * 1024 * 1024;
    }

    public void tryReconstructSourceUsageHistory() {
        SourceUsageRecord usage = getLatestRecord();
        if (usage == null) {
            updateSourceUsage();
        }
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
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getAllResources(ResourceStore.HISTORY_SOURCE_USAGE, from, Long.MAX_VALUE, SOURCE_USAGE_SERIALIZER);
    }

    public List<SourceUsageRecord> getLatestRecordByHours(int hoursAgo) {
        return getLatestRecordByMs(hoursAgo * 3600L * 1000L);
    }

    private int getThresholdByDayFromOrigin() {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date = simpleDateFormat.parse("1970-01-01");
            return (int) ((System.currentTimeMillis() - date.getTime()) / (1000 * 60 * 60 * 24));
        } catch (ParseException e) {
            logger.error("parser date error", e);
            return 365 * 10;
        }
    }

    // return all records in last one month
    public List<SourceUsageRecord> getLastMonthRecords() {
        return getLatestRecordByHours(24 * 30);
    }
}
