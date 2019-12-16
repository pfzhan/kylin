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

package io.kyligence.kap.smart.util;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.SparderTypeUtil;

import com.google.common.base.Preconditions;

import io.kyligence.kap.engine.spark.builder.CreateFlatTable$;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.BadModelException;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnEvalUtil {

    private static final String CC_NAME_PREFIX = "CC_AUTO_";
    public static final String DEFAULT_CC_NAME = "CC_AUTO_1";

    private static final Pattern PATTERN_COLUMN = Pattern
            .compile("cannot resolve '(.*?)' given input columns: \\[(.*?),(.*?)];");

    private ComputedColumnEvalUtil() {
        throw new IllegalAccessError();
    }

    private static void throwIllegalStateException(List<ComputedColumnDesc> computedColumns, Exception e) {
        Preconditions.checkNotNull(computedColumns);
        // other exception occurs, fail directly
        throw new IllegalStateException("Cannot evaluate data type of computed column "
                + computedColumns.stream().map(ComputedColumnDesc::getInnerExpression).collect(Collectors.joining(","))
                + " due to unsupported expression.", e);
    }

    public static void evaluateExprAndTypeBatch(NDataModel nDataModel, List<ComputedColumnDesc> computedColumns) {
        evalDataTypeOfCCInAuto(computedColumns, nDataModel, 0, computedColumns.size());
        computedColumns.removeIf(cc -> cc.getDatatype().equals("ANY"));
    }

    public static void evaluateExprAndType(NDataModel nDataModel, ComputedColumnDesc computedColumn) {
        evalDataTypeOfCCInManual(Collections.singletonList(computedColumn), nDataModel, 0, 1);
    }

    private static void evalDataTypeOfCCInAuto(List<ComputedColumnDesc> computedColumns, NDataModel nDataModel,
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

    private static void evalDataTypeOfCCInManual(List<ComputedColumnDesc> computedColumns, NDataModel nDataModel,
            int start, int end) {
        try {
            evalDataTypeOfCC(computedColumns, SparderEnv.getSparkSession(), nDataModel, start, end);
        } catch (AnalysisException e) {
            Matcher matcher = PATTERN_COLUMN.matcher(e.getMessage());
            if (matcher.find()) {
                String str = matcher.group(2).replace(NSparkCubingUtil.SEPARATOR, ".");
                if (KylinConfig.getInstanceFromEnv().isUTEnv()) { // for test
                    throw new IllegalStateException("Cannot find column " //
                            + matcher.group(1).replace(NSparkCubingUtil.SEPARATOR, ".")
                            + ", please check whether schema of related table has changed.", e);
                } else {
                    throw new IllegalStateException("Cannot find column " //
                            + str.substring(0, str.lastIndexOf('.')) + "." + matcher.group(1)
                            + ", please check whether schema of related table has changed.", e);
                }
            }

            throwIllegalStateException(computedColumns, e);
        }
    }

    private static void evalDataTypeOfCC(List<ComputedColumnDesc> computedColumns, SparkSession ss,
            NDataModel nDataModel, int start, int end) throws AnalysisException {
        Dataset<Row> originDf = CreateFlatTable$.MODULE$.generateFullFlatTable(nDataModel, ss).limit(10);
        originDf.persist();
        Dataset<Row> ds = originDf.selectExpr(computedColumns.subList(start, end).stream() //
                .map(ComputedColumnDesc::getInnerExpression) //
                .map(NSparkCubingUtil::convertFromDot).toArray(String[]::new));
        for (int i = start; i < end; i++) {
            String dataType = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i - start].dataType());
            computedColumns.get(i).setDatatype(dataType);
        }
    }

    public static boolean resolveCCName(ComputedColumnDesc ccDesc, NDataModel dataModel, List<NDataModel> otherModels,
            KylinConfig config, String project) {
        // Resolve CC name, Limit 99 retries to avoid infinite loop
        // TODO: what if the dataModel has more than 99 computed columns?
        int retryCount = 0;
        while (retryCount < 99) {
            retryCount++;
            try {
                // Init model to check CC availability
                dataModel.init(config, NTableMetadataManager.getInstance(config, project).getAllTablesMap(),
                        otherModels, project);
                // No exception, check passed
                return true;
            } catch (BadModelException e) {
                switch (e.getCauseType()) {
                case SAME_NAME_DIFF_EXPR:
                case WRONG_POSITION_DUE_TO_NAME:
                case SELF_CONFLICT:
                    // updating CC auto index to resolve name conflict
                    String ccName = ccDesc.getColumnName();
                    ccDesc.setColumnName(incrementIndex(ccName));
                    break;
                case SAME_EXPR_DIFF_NAME:
                    ccDesc.setColumnName(e.getAdvise());
                    break;
                case WRONG_POSITION_DUE_TO_EXPR:
                case LOOKUP_CC_NOT_REFERENCING_ITSELF:
                    log.debug("Bad CC suggestion: {}", ccDesc.getExpression(), e);
                    retryCount = 99; // fail directly
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
        if (oldAlias == null || !oldAlias.startsWith(CC_NAME_PREFIX) || oldAlias.equals(CC_NAME_PREFIX)) {
            return DEFAULT_CC_NAME;
        }

        String idxStr = oldAlias.substring(CC_NAME_PREFIX.length());
        Integer idx;
        try {
            idx = Integer.valueOf(idxStr);
        } catch (NumberFormatException e) {
            return DEFAULT_CC_NAME;
        }

        idx++;
        return CC_NAME_PREFIX + idx.toString();
    }
}
