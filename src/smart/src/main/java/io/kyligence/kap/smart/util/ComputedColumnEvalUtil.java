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

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.util.SparderTypeUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.builder.CreateFlatTable$;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnEvalUtil {

    private ComputedColumnEvalUtil() {
        throw new IllegalAccessError();
    }

    public static void evaluateExprAndTypes(NDataModel nDataModel, List<ComputedColumnDesc> computedColumns) {
        Map<String, ComputedColumnDesc> expToCcMap = Maps.newConcurrentMap();
        computedColumns.forEach(cc -> expToCcMap.putIfAbsent(cc.getInnerExpression(), cc));
        SparkSession ss = SparderEnv.getSparkSession();
        while (true) {
            try {
                Dataset<Row> ds = CreateFlatTable$.MODULE$.generateFullFlatTable(nDataModel, ss).selectExpr(
                        expToCcMap.keySet().stream().map(NSparkCubingUtil::convertFromDot).toArray(String[]::new));
                computedColumns.removeIf(cc -> !expToCcMap.containsKey(cc.getInnerExpression()));
                for (int i = 0; i < computedColumns.size(); i++) {
                    String dataType = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i].dataType());
                    computedColumns.get(i).setDatatype(dataType);
                }
                break;
            } catch (Exception e) {
                if (e instanceof ParseException) {
                    final int size = expToCcMap.size();
                    String parseCmdInfo = ((ParseException) e).command().getOrElse(null);
                    expToCcMap.forEach((exp, cc) -> {
                        if (parseCmdInfo == null || parseCmdInfo.contains(NSparkCubingUtil.convertFromDot(exp))) {
                            expToCcMap.remove(exp);
                            log.warn("Unsupported to infer CC type, corresponding cc expression is: {}", exp);
                        }
                    });

                    Preconditions.checkState(size != expToCcMap.size(),
                            "[UNLIKELY_THINGS_HAPPENED] ParseException occurs, but no cc expression was removed, {}",
                            e);
                } else {
                    // other exception occurs, fail directly
                    throw new IllegalStateException("Auto model failed to evaluate CC "
                            + String.join(",", expToCcMap.keySet()) + ", CC expression not valid.", e);
                }
            }
        }
    }
}
