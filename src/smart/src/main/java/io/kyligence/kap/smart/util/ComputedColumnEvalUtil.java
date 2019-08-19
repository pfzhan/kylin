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

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.util.SparderTypeUtil;

import com.google.common.base.Preconditions;

import io.kyligence.kap.engine.spark.builder.CreateFlatTable$;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnEvalUtil {

    private static final Pattern PATTERN_COLUMN = Pattern
            .compile("(cannot resolve '(.*?)' given input columns: \\[(.*?)\\.(.*?)\\..*?[,|\\]|\\s])");

    private ComputedColumnEvalUtil() {
        throw new IllegalAccessError();
    }

    private static void throwIllegalStateException(List<ComputedColumnDesc> computedColumns, Exception e) {
        Preconditions.checkNotNull(computedColumns);

        // other exception occurs, fail directly
        throw new IllegalStateException("Auto model failed to evaluate CC " + String.join(",",
                computedColumns.stream().map(ComputedColumnDesc::getInnerExpression).collect(Collectors.toList()))
                + ", CC expression not valid.", e);
    }

    private static void dealWithParseException(List<ComputedColumnDesc> computedColumns, ParseException e) {
        Preconditions.checkNotNull(computedColumns);

        int size = computedColumns.size();
        String parseCmdInfo = e.command().getOrElse(null);
        Iterator<ComputedColumnDesc> iterator = computedColumns.iterator();
        while (iterator.hasNext()) {
            ComputedColumnDesc cc = iterator.next();
            if (parseCmdInfo == null
                    || parseCmdInfo.contains(NSparkCubingUtil.convertFromDot(cc.getInnerExpression()))) {
                log.warn("Unsupported to infer CC type, corresponding cc expression is: {}", cc.getInnerExpression());
                iterator.remove();
            }
        }

        Preconditions.checkState(size != computedColumns.size(),
                "[UNLIKELY_THINGS_HAPPENED] ParseException occurs, but no cc expression was removed, {}", e);
    }

    public static void evaluateExprAndTypes(NDataModel nDataModel, List<ComputedColumnDesc> computedColumns) {
        SparkSession ss = SparderEnv.getSparkSession();
        while (true) {
            try {
                Dataset<Row> ds = CreateFlatTable$.MODULE$.generateFullFlatTable(nDataModel, ss)
                        .selectExpr(computedColumns.stream() //
                                .map(ComputedColumnDesc::getInnerExpression) //
                                .map(NSparkCubingUtil::convertFromDot).toArray(String[]::new));
                for (int i = 0; i < computedColumns.size(); i++) {
                    String dataType = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i].dataType());
                    computedColumns.get(i).setDatatype(dataType);
                }
                break;
            } catch (ParseException e) {
                dealWithParseException(computedColumns, e);
            } catch (AnalysisException e) {
                Matcher matcher = PATTERN_COLUMN.matcher(e.getMessage());
                if (matcher.find()) {
                    throw new IllegalStateException(String.format(
                            "Failed to evaluate '%s.%s' in computed column check, please check whether the table structure has been changed.",
                            matcher.group(4), matcher.group(2)), e);
                }

                throwIllegalStateException(computedColumns, e);
            } catch (Exception e) {
                throwIllegalStateException(computedColumns, e);
            }
        }
    }
}
