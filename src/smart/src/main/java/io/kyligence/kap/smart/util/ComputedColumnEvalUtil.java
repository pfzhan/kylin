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
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.SparderTypeUtil;

import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;

public class ComputedColumnEvalUtil {
    
    private ComputedColumnEvalUtil() {
        throw new IllegalAccessError();
    }

    public static void evaluateExprAndTypes(NDataModel nDataModel, List<ComputedColumnDesc> computedColumns) {
        List<String> expressions = computedColumns.stream().map(ComputedColumnDesc::getInnerExpression)
                .collect(Collectors.toList());
        String cols = StringUtils.join(expressions, ",");
        try {
            SparkSession ss = SparderEnv.getSparkSession();
            Dataset<Row> ds = NJoinedFlatTable.generateDataset(nDataModel, ss)
                    .selectExpr(expressions.stream().map(NSparkCubingUtil::convertFromDot).toArray(String[]::new));
            for (int i = 0; i < computedColumns.size(); i++) {
                String dataType = SparderTypeUtil.convertSparkTypeToSqlType(ds.schema().fields()[i].dataType());
                computedColumns.get(i).setDatatype(dataType);
            }
        } catch (Exception e) {
            // Fail directly if error in validating SQL
            throw new IllegalStateException(
                    "Auto model failed to evaluate CC " + cols + ", CC expression not valid.", e);
        }
    }
}
