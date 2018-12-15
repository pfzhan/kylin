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

package org.apache.kylin.query.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.query.util.QueryUtil.IQueryTransformer;

public class DefaultQueryTransformer implements IQueryTransformer {

    private static final String S0 = "\\s*";
    private static final String S1 = "\\s";
    private static final String SM = "\\s+";
    private static final Pattern PTN_GROUP_BY = Pattern.compile(S1 + "GROUP" + SM + "BY" + S1,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_HAVING_COUNT_GREATER_THAN_ZERO = Pattern.compile(S1 + "HAVING" + SM + "[(]?" + S0
            + "COUNT" + S0 + "[(]" + S0 + "1" + S0 + "[)]" + S0 + ">" + S0 + "0" + S0 + "[)]?",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_SUM = Pattern
            .compile(S0 + "SUM" + S0 + "[(]" + S0 + "\\d+(\\.\\d+)?" + S0 + "[)]" + S0, Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_SUM_1 = Pattern.compile(S0 + "SUM" + S0 + "[(]" + S0 + "[1]" + S0 + "[)]" + S0,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_MIN_1 = Pattern.compile(S0 + "MIN" + S0 + "[(]" + S0 + "[1]" + S0 + "[)]" + S0,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_MAX_1 = Pattern.compile(S0 + "MAX" + S0 + "[(]" + S0 + "[1]" + S0 + "[)]" + S0,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_NOT_EQ = Pattern.compile(S0 + "!=" + S0, Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_INTERVAL = Pattern.compile(
            "interval" + SM + "(floor\\()([\\d\\.]+)(\\))" + SM + "(second|minute|hour|day|month|year)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_HAVING_ESCAPE_FUNCTION = Pattern.compile("\\{fn" + SM + "(EXTRACT\\(.*?\\))" + "\\}",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PIN_SUM_OF_CAST = Pattern.compile(S0 + "SUM" + S0 + "\\(" + S0 + "CAST" + S0 + "\\("
            + S0 + "([^\\s,]+)" + S0 + "AS" + SM + "DOUBLE" + S0 + "\\)" + S0 + "\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PIN_SUM_OF_FN_CONVERT = Pattern
            .compile(S0 + "SUM" + S0 + "\\(" + S0 + "\\{\\s*fn" + SM + "convert" + S0 + "\\(" + S0 + "([^\\s,]+)" + S0
                    + "," + S0 + "(SQL_DOUBLE|DOUBLE|SQL_BIGINT|BIGINT|INT|SMALLINT|SQL_SMALLINT" +
                    "|TINYINT|SQL_TINYINT|INTEGER|SQL_INTEGER|FLOAT|SQL_FLOAT)" + S0 + "\\)"
                    + S0 + "\\}" + S0 + "\\)", Pattern.CASE_INSENSITIVE);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        Matcher m;

        // Case: SUM(CAST (column_name AS DOUBLE))
        while (true) {
            m = PIN_SUM_OF_CAST.matcher(sql);
            if (!m.find())
                break;

            sql = sql.substring(0, m.start()) + " SUM(" + m.group(1).trim() + ")"
                    + sql.substring(m.end(), sql.length());
        }

        //Case: SUM({fn CONVERT(...)}) generated by PowerBI
        while (true) {
            m = PIN_SUM_OF_FN_CONVERT.matcher(sql);
            if (!m.find())
                break;

            sql = sql.substring(0, m.start()) + " SUM(" + m.group(1).trim() + ")"
                    + sql.substring(m.end(), sql.length());
        }

        // Case {fn EXTRACT(...) }
        // Use non-greedy regex matching to remove escape functions
        // Notice: Only unsupported escape function need to be handled
        // Reference: https://calcite.apache.org/docs/reference.html#jdbc-function-escape
        while (true) {
            m = PTN_HAVING_ESCAPE_FUNCTION.matcher(sql);
            if (!m.find())
                break;
            sql = sql.substring(0, m.start()) + m.group(1) + sql.substring(m.end());
        }

        // Case: HAVING COUNT(1)>0 without Group By
        // Tableau generates: SELECT SUM(1) AS "COL" FROM "VAC_SW" HAVING
        // COUNT(1)>0
        m = PTN_HAVING_COUNT_GREATER_THAN_ZERO.matcher(sql);
        if (m.find() && PTN_GROUP_BY.matcher(sql).find() == false) {
            sql = sql.substring(0, m.start()) + " " + sql.substring(m.end());
        }

        // Case: SUM(1)
        // Replace it with COUNT(1)
        while (true) {
            m = PTN_SUM_1.matcher(sql);
            if (!m.find())
                break;
            sql = sql.substring(0, m.start()) + " COUNT(1) " + sql.substring(m.end());
        }

        // Case: SUM(N)
        // Replace it with N * COUNT(1)
        while (true) {
            m = PTN_SUM.matcher(sql);
            if (!m.find())
                break;
            String val = m.group().toUpperCase().replace("SUM(", "").replace(")", "");
            sql = sql.substring(0, m.start()) + " " + val.trim() + " * COUNT(1) " + sql.substring(m.end());
        }

        // Case: MIN(1) or MAX(1)
        // Replace it with 1
        while (true) {
            m = PTN_MIN_1.matcher(sql);
            if (!m.find())
                break;
            sql = sql.substring(0, m.start()) + " 1 " + sql.substring(m.end());
        }
        while (true) {
            m = PTN_MAX_1.matcher(sql);
            if (!m.find())
                break;
            sql = sql.substring(0, m.start()) + " 1 " + sql.substring(m.end());
        }

        // Case: !=
        // Replace it with <>
        while (true) {
            m = PTN_NOT_EQ.matcher(sql);
            if (!m.find())
                break;
            sql = sql.substring(0, m.start()) + " <> " + sql.substring(m.end());
        }

        // ( date '2001-09-28' + interval floor(1) day ) generated by cognos
        // calcite only recognizes date '2001-09-28' + interval '1' day
        while (true) {
            m = PTN_INTERVAL.matcher(sql);
            if (!m.find())
                break;

            int value = (int) Math.floor(Double.valueOf(m.group(2)));
            sql = sql.substring(0, m.start(1)) + "'" + value + "'" + sql.substring(m.end(3));
        }

        
        return sql;
    }
}
