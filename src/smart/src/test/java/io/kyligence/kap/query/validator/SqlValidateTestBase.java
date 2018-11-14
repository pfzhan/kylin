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

package io.kyligence.kap.query.validator;

import java.util.Arrays;
import java.util.Map;

import io.kyligence.kap.smart.common.NTestBase;
import io.kyligence.kap.smart.query.ResultDetails;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

class SqlValidateTestBase extends NTestBase {
    void printSqlValidateResults(Map<String, SQLValidateResult> validateStatsMap) {
        validateStatsMap.forEach((key, sqlValidateResult) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("sql: ").append(key).append(",\n\t");
            sb.append("capable: ").append(sqlValidateResult.isCapable()).append(",\n\t");

            sqlValidateResult.getSqlAdvices().forEach(sqlAdvice -> {
                sb.append("reason:").append(sqlAdvice.getIncapableReason()).append("\n\t");
                sb.append("suggest:").append(sqlAdvice.getSuggestion()).append("\n\t");
            });

            ResultDetails details = sqlValidateResult.getResult().getDetails();
            sb.append("cuboidIds: ").append(details.getCuboidIds()).append("\n\t");
            sb.append("realizations: ").append(Arrays.toString(details.getRealizationNames().toArray(new String[0])))
                    .append("\n\t");
            sb.append("total scanned bytes: ").append(details.getScannedBytes()).append("\n\t");
            sb.append("total scanned rows: ").append(details.getScannedRows()).append("\n\t");
            sb.append("result row count: ").append(details.getResultRowCount()).append("\n\t");
            sb.append("project: ").append(details.getProject()).append("\n\t");
            sb.append("duration: ").append(details.getDuration()).append("\n\t");
            sb.append("query id: ").append(details.getQueryId()).append("\n\t");

            System.out.println(sb.toString());
        });
    }
}
