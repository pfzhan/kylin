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

package io.kyligence.kap.engine.spark.utils;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.utils.SchemaProcessor;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.builder.CreateFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.exception.IllegalCCExpressionException;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnEvalUtil {
    private static final int MAX_RENAME_CC_TIME = 99;

    private static final Pattern PATTERN_COLUMN = Pattern
            .compile("cannot resolve '(.*?)' given input columns: \\[(.*?),(.*?)];");

    private ComputedColumnEvalUtil() {
        throw new IllegalAccessError();
    }

    public static void evaluateExprAndTypeBatch(NDataModel nDataModel, List<ComputedColumnDesc> computedColumns) {
        evalDataTypeOfCCInAuto(computedColumns, nDataModel, 0, computedColumns.size());
        computedColumns.removeIf(cc -> cc.getDatatype().equals("ANY"));
    }

    public static void evaluateExprAndType(NDataModel nDataModel, ComputedColumnDesc computedColumn) {
        evalDataTypeOfCCInManual(Collections.singletonList(computedColumn), nDataModel, 0, 1);
    }

    public static void evalDataTypeOfCCInAuto(List<ComputedColumnDesc> computedColumns, NDataModel nDataModel,
            int start, int end) {
        try {
            evalDataTypeOfCC(computedColumns, SparderEnv.getSparkSession(), nDataModel, start, end);
        } catch (Exception e) {
            if (end - start > 1) { //numbers of CC > 1
                evalDataTypeOfCCInAuto(computedColumns, nDataModel, start, start + (end - start) / 2);
                evalDataTypeOfCCInAuto(computedColumns, nDataModel, start + (end - start) / 2, end);
            } else { //numbers of CC = 1
                computedColumns.get(start).setDatatype("ANY");
                log.info("Discard the computed column {} for {}", computedColumns.get(start).getInnerExpression(),
                        e.getMessage());
            }
        }
    }

    public static void evalDataTypeOfCCInBatch(NDataModel nDataModel, List<ComputedColumnDesc> computedColumns) {
        if (CollectionUtils.isEmpty(computedColumns)) {
            return;
        }
        try {
            evalDataTypeOfCC(computedColumns, SparderEnv.getSparkSession(), nDataModel, 0, computedColumns.size());
        } catch (AnalysisException e) {
            evalDataTypeOfCCInManual(computedColumns, nDataModel, 0, computedColumns.size());
        }
    }

    private static void evalDataTypeOfCCInManual(List<ComputedColumnDesc> computedColumns, NDataModel nDataModel,
            int start, int end) {
        for (int i = start; i < end; i++) {
            try {
                evalDataTypeOfCC(computedColumns, SparderEnv.getSparkSession(), nDataModel, i, i + 1);
            } catch (AnalysisException e) {
                Matcher matcher = PATTERN_COLUMN.matcher(e.getMessage());
                if (matcher.find()) {
                    String str = matcher.group(2).replace(NSparkCubingUtil.SEPARATOR, ".");
                    throw new IllegalCCExpressionException(QueryErrorCode.CC_EXPRESSION_ILLEGAL,
                            "Cannot find column " + str.substring(0, str.lastIndexOf('.')) + "." + matcher.group(1)
                                    + ", please check whether schema of related table has changed.");
                }

                Preconditions.checkNotNull(computedColumns.get(i));
                throw new IllegalCCExpressionException(QueryErrorCode.CC_EXPRESSION_ILLEGAL,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getCheckCCExpression(),
                                computedColumns.get(i).getTableAlias() + "." + computedColumns.get(i).getColumnName(),
                                computedColumns.get(i).getExpression()));
            }
        }
    }

    private static void evalDataTypeOfCC(List<ComputedColumnDesc> computedColumns, SparkSession ss,
                                         NDataModel nDataModel, int start, int end) throws AnalysisException {
        val originDf = generateFullFlatTableDF(ss, nDataModel);
        originDf.persist();
        Dataset<Row> ds = originDf.selectExpr(computedColumns.subList(start, end).stream() //
                .map(ComputedColumnDesc::getInnerExpression) //
                .map(NSparkCubingUtil::convertFromDotWithBackticks).toArray(String[]::new));
        for (int i = start; i < end; i++) {
            String dataType = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i - start].dataType());
            computedColumns.get(i).setDatatype(dataType);
        }
    }

    private static Dataset<Row> generateFullFlatTableDF(SparkSession ss, NDataModel model) {
        // root fact table
        val rootDF = generateDatasetOnTable(ss, model.getRootFactTable());

        // look up tables
        val joinTableDFMap = Maps.<JoinTableDesc, Dataset<Row>> newLinkedHashMap();
        model.getJoinTables().forEach(
                joinTable -> joinTableDFMap.put(joinTable, generateDatasetOnTable(ss, joinTable.getTableRef())));

        return CreateFlatTable.joinFactTableWithLookupTables(rootDF, joinTableDFMap, model, ss);
    }

    private static Dataset<Row> generateDatasetOnTable(SparkSession ss, TableRef tableRef) {
        val tableCols = tableRef.getColumns().stream().map(TblColRef::getColumnDesc)
                .filter(col -> !col.isComputedColumn()).toArray(ColumnDesc[]::new);
        val structType = SchemaProcessor.buildSchemaWithRawTable(tableCols);
        val alias = tableRef.getAlias();
        val dataset = ss.createDataFrame(Lists.newArrayList(), structType).alias(alias);
        return CreateFlatTable.changeSchemaToAliasDotName(dataset, alias);
    }

    public static boolean resolveCCName(ComputedColumnDesc ccDesc, NDataModel dataModel, List<NDataModel> otherModels) {
        // Resolve CC name, Limit MAX_RENAME_CC_TIME retries to avoid infinite loop
        // TODO: what if the dataModel has more than MAX_RENAME_CC_TIME computed columns?
        int retryCount = 0;
        while (retryCount < MAX_RENAME_CC_TIME) {
            retryCount++;
            try {
                // Init ComputedColumn to check CC availability
                dataModel.initComputedColumns(otherModels);
                // No exception, check passed
                return true;
            } catch (BadModelException e) {
                switch (e.getCauseType()) {
                case SAME_NAME_DIFF_EXPR:
                case WRONG_POSITION_DUE_TO_NAME:
                case SELF_CONFLICT_WITH_SAME_NAME:
                    // updating CC auto index to resolve name conflict
                    String ccName = ccDesc.getColumnName();
                    ccDesc.setColumnName(incrementIndex(ccName));
                    break;
                case SAME_EXPR_DIFF_NAME:
                    ccDesc.setColumnName(e.getAdvise());
                    break;
                case WRONG_POSITION_DUE_TO_EXPR:
                case LOOKUP_CC_NOT_REFERENCING_ITSELF:
                case SELF_CONFLICT_WITH_SAME_EXPRESSION:
                    log.debug("Bad CC suggestion: {}", ccDesc.getExpression(), e);
                    retryCount = MAX_RENAME_CC_TIME; // fail directly
                    break;
                default:
                    break;
                }
            } catch (Exception e) {
                log.debug("When resolving the name of computed column {}, model {} initializing failed.", //
                        ccDesc, dataModel.getUuid(), e);
                break; // break loop
            }
        }
        return false;
    }

    private static String incrementIndex(String oldAlias) {
        if (oldAlias == null || !oldAlias.startsWith(ComputedColumnUtil.CC_NAME_PREFIX)
                || oldAlias.equals(ComputedColumnUtil.CC_NAME_PREFIX)) {
            return ComputedColumnUtil.DEFAULT_CC_NAME;
        }

        String idxStr = oldAlias.substring(ComputedColumnUtil.CC_NAME_PREFIX.length());
        int idx;
        try {
            idx = Integer.parseInt(idxStr);
        } catch (NumberFormatException e) {
            return ComputedColumnUtil.DEFAULT_CC_NAME;
        }

        idx++;
        return ComputedColumnUtil.CC_NAME_PREFIX + idx;
    }

}
