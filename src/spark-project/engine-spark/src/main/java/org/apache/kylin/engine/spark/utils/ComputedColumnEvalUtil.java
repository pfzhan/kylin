/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.utils;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.engine.spark.job.NSparkCubingUtil;
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.model.BadModelException;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.exception.IllegalCCExpressionException;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.springframework.util.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnEvalUtil {
    private static final int MAX_RENAME_CC_TIME = 99;

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
            evalDataTypeOfCC(computedColumns, nDataModel, start, end);
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
            evalDataTypeOfCC(computedColumns, nDataModel, 0, computedColumns.size());
        } catch (Exception e) {
            evalDataTypeOfCCInManual(computedColumns, nDataModel, 0, computedColumns.size());
        }
    }

    private static void evalDataTypeOfCCInManual(List<ComputedColumnDesc> computedColumns, NDataModel nDataModel,
            int start, int end) {
        for (int i = start; i < end; i++) {
            try {
                evalDataTypeOfCC(computedColumns, nDataModel, i, i + 1);
            } catch (Exception e) {
                Preconditions.checkNotNull(computedColumns.get(i));
                throw new IllegalCCExpressionException(QueryErrorCode.CC_EXPRESSION_ILLEGAL,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getCheckCCExpression(),
                                computedColumns.get(i).getTableAlias() + "." + computedColumns.get(i).getColumnName(),
                                computedColumns.get(i).getExpression()));
            }
        }
    }

    private static void evalDataTypeOfCC(List<ComputedColumnDesc> computedColumns, NDataModel nDataModel, int start,
            int end) {
        IndexDependencyParser parser = new IndexDependencyParser(nDataModel);
        Dataset<Row> df = parser.getFullFlatTableDataFrame(nDataModel);
        String[] ccExprArray = computedColumns.subList(start, end).stream() //
                .map(ComputedColumnDesc::getInnerExpression) //
                .map(NSparkCubingUtil::convertFromDotWithBackTick).toArray(String[]::new);
        Dataset<Row> ds = df.selectExpr(ccExprArray);
        for (int i = start; i < end; i++) {
            String dataType = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i - start].dataType());
            computedColumns.get(i).setDatatype(dataType);
        }
    }

    public static boolean resolveCCName(ComputedColumnDesc ccDesc, NDataModel dataModel, List<NDataModel> otherModels) {
        // Resolve CC name, Limit MAX_RENAME_CC_TIME retries to avoid infinite loop.
        // What if the dataModel has more than MAX_RENAME_CC_TIME computed columns?
        int retryCount = 0;
        while (retryCount < MAX_RENAME_CC_TIME) {
            retryCount++;
            try {
                // Init ComputedColumn to check CC availability
                dataModel.initComputedColumnsFailFast(otherModels);
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
