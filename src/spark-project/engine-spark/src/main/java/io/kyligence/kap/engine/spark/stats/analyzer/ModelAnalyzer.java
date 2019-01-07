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

package io.kyligence.kap.engine.spark.stats.analyzer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.builder.CreateFlatTable;
import io.kyligence.kap.engine.spark.builder.NSizeEstimator;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.DataCheckDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;

public class ModelAnalyzer implements Serializable {

    private transient final Logger logger = LoggerFactory.getLogger(getClass());

    private final NDataModel dataModel;

    private final KylinConfig config;

    public ModelAnalyzer(NDataModel dataModel, KylinConfig config) {
        this.dataModel = dataModel;
        this.config = config;
    }

    public void analyze(NDataSegment segment, SparkSession ss) throws IOException {
        final DataCheckDesc checkDesc = dataModel.getDataCheckDesc();
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config,
                dataModel.getProject());

        // analysis root fact table
        final TableRef factTableRel = dataModel.getRootFactTable();
        final Dataset<Row> factTableData = getTableDataWithRange(factTableRel, segment, ss);
        final TableAnalyzerResult factTableAnalyzerResult = analysisTable(factTableData, factTableRel.getTableDesc());
        saveOrUpdateTableStats(factTableAnalyzerResult, segment.getSegRange());

        // analysis lookup tables
        for (final JoinTableDesc lookupDesc : dataModel.getJoinTables()) {
            final TableRef lookupTableRef = lookupDesc.getTableRef();
            final TableExtDesc tableExt = tableMetadataManager.getTableExtIfExists(lookupTableRef.getTableDesc());
            if (tableExt == null || CollectionUtils.isEmpty(tableExt.getColumnStats())
                    || checkDesc.checkForceAnalysisLookup()) {

                final Dataset<Row> lookupTableData = getTableData(lookupTableRef, ss);
                final TableAnalyzerResult lookupTableAnalyzerResult = analysisTable(lookupTableData,
                        lookupTableRef.getTableDesc());
                saveOrUpdateTableStats(lookupTableAnalyzerResult, null);
            }
        }

        if (checkDesc.checkDuplicatePK()) {
            // TODO #6800 check lookup table duplicate PK
        }

        if (checkDesc.checkDataSkew()) {
            // TODO #6800  check data skew
        }

    }

    private Dataset<Row> getTableDataWithRange(TableRef tableRef, NDataSegment segment, SparkSession ss) {
        final Dataset<Row> dataset = getTableData(tableRef, ss);

        final PartitionDesc partitionDesc = getPartitionDesc(dataModel);
        if (partitionDesc == null || partitionDesc.getPartitionDateColumnRef() == null) {
            // TODO full check or sample?
            logger.warn("Can not found model {}'s date partition desc, full check", dataModel.getUuid());
            return dataset;
        }

        final SegmentRange segRange = segment.getSegRange();
        if (segRange == null || segRange.isInfinite()) {
            // TODO full check or sample?
            logger.warn("Segment {}'s range is infinite, full check", segment.getName());
            return dataset;
        }

        final String dateRangeCondition = CreateFlatTable.replaceDot(
                partitionDesc.getPartitionConditionBuilder().buildDateRangeCondition(partitionDesc, segment, segRange),
                dataModel);
        logger.info("Select date range condition [{}]", dateRangeCondition);

        return dataset.where(dateRangeCondition);
    }

    private PartitionDesc getPartitionDesc(NDataModel dataModel) {
        PartitionDesc partitionDesc = dataModel.getPartitionDesc();
        if (partitionDesc == null || partitionDesc.getPartitionDateColumnRef() == null) {
            final NDataLoadingRangeManager dataRangeManager = NDataLoadingRangeManager.getInstance(config,
                    dataModel.getProject());
            final NDataLoadingRange dataLoadingRange = dataRangeManager
                    .getDataLoadingRange(dataModel.getRootFactTableName());
            if (dataLoadingRange == null)
                return null;

            TblColRef partitionCol = dataModel.findColumn(dataLoadingRange.getColumnName());
            if (partitionCol == null || !partitionCol.getType().isDate()) {
                return null;
            }

            partitionDesc = new PartitionDesc();
            partitionDesc.setPartitionDateColumn(partitionCol.getIdentity());
            partitionDesc.init(dataModel);
        }

        return partitionDesc;
    }

    private Dataset<Row> getTableData(TableRef tableRef, SparkSession ss) {
        final TableDesc tableDesc = tableRef.getTableDesc();
        final Dataset<Row> dataset = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, Maps.newHashMap()).alias(tableRef.getAlias());

        logger.info("Loading table {} sampling data. \n {}", tableDesc.getName(), dataset.schema().treeString());

        return CreateFlatTable.changeSchemaToAliasDotName(dataset, tableRef.getAlias());
    }

    private TableAnalyzerResult analysisTable(final Dataset<Row> tableDS, final TableDesc tableDesc) {
        final int partition = estimatePartitions(tableDS, config);
        logger.info("Analysing table {}, repartition with size {}", tableDesc.getName(), partition);
        return tableDS.toJavaRDD().repartition(partition)
                .mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>, Iterator<TableAnalyzerResult>>() {
                    @Override
                    public Iterator<TableAnalyzerResult> call(Integer index, Iterator<Row> rowIterator)
                            throws Exception {
                        final TableAnalyzer tableAnalyzer = new TableAnalyzer(tableDesc);
                        while (rowIterator.hasNext()) {
                            tableAnalyzer.analyze(rowIterator.next());
                        }
                        final TableAnalyzerResult analyzerResult = tableAnalyzer.getResult();
                        System.out.println("Analysing table " + tableDesc.getName() + " with partition " + partition
                                + ", row size: " + analyzerResult.getRowCount());
                        return Iterators.singletonIterator(analyzerResult);
                    }
                }, false).reduce(new Function2<TableAnalyzerResult, TableAnalyzerResult, TableAnalyzerResult>() {
                    @Override
                    public TableAnalyzerResult call(TableAnalyzerResult v1, TableAnalyzerResult v2) throws Exception {
                        return TableAnalyzerResult.reduce(v1, v2);
                    }
                });
    }

    private void saveOrUpdateTableStats(TableAnalyzerResult analyzerResult, SegmentRange segRange) throws IOException {
        final TableDesc tableDesc = analyzerResult.getTableDesc();

        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config,
                dataModel.getProject());
        val tableExt = tableMetadataManager.getOrCreateTableExt(tableDesc);

        // for lookup table
        boolean isLookup = false;
        if (segRange == null) {
            segRange = SourceFactory.getSource(tableDesc).getSegmentRange("", "");
            isLookup = true;
        }

        final boolean isAppend = !tableExt.getLoadingRange().contains(segRange);
        if (isAppend) {
            tableExt.updateLoadingRange(segRange);
            tableExt.setTotalRows(tableExt.getTotalRows() + analyzerResult.getRowCount());
        }

        // corrected lookup table's total rows
        if (isLookup) {
            tableExt.setTotalRows(analyzerResult.getRowCount());
        }

        final List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>(tableDesc.getColumnCount());
        for (int colIdx = 0; colIdx < tableDesc.getColumnCount(); colIdx++) {
            final ColumnDesc columnDesc = tableDesc.getColumns()[colIdx];
            if (columnDesc.isComputedColumn()) {
                continue;
            }
            TableExtDesc.ColumnStats colStats = tableExt.getColumnStats(colIdx);
            if (colStats == null) {
                colStats = new TableExtDesc.ColumnStats();
                colStats.setColumnName(columnDesc.getName());
            }

            if (isAppend) {
                colStats.addNullCount(analyzerResult.getNullOrBlankCount(colIdx));
            }

            // corrected lookup table's null or blank count
            if (isLookup) {
                colStats.setNullCount(analyzerResult.getNullOrBlankCount(colIdx));
            }

            colStats.addRangeHLLC(segRange, analyzerResult.getHLLCBytes(colIdx));

            final double maxValue = columnDesc.getType().isNumberFamily() ? analyzerResult.getMaxNumeral(colIdx)
                    : Double.NaN;
            final double minValue = columnDesc.getType().isNumberFamily() ? analyzerResult.getMinNumeral(colIdx)
                    : Double.NaN;
            final int maxLength = analyzerResult.getMaxLength(colIdx);
            final int minLength = analyzerResult.getMinLength(colIdx);
            final String maxLengthValue = analyzerResult.getMaxLengthValue(colIdx);
            final String minLengthValue = analyzerResult.getMinLengthValue(colIdx);
            colStats.updateBasicStats(maxValue, minValue, maxLength, minLength, maxLengthValue, minLengthValue);

            columnStatsList.add(colStats);
        }

        tableExt.setColumnStats(columnStatsList);
        tableMetadataManager.saveTableExt(tableExt);
        logger.info("Table {} analysis finished, update table ext desc done.", tableDesc.getName());
    }

    public static int estimatePartitions(Dataset<Row> ds, KylinConfig config) {
        int sizeMB = (int) (NSizeEstimator.estimate(ds, 0.1f) / (1024 * 1024));
        int partition = sizeMB / KapConfig.wrap(config).getParquetStorageShardSize();
        if (partition == 0)
            partition = 1;
        return partition;
    }
}
