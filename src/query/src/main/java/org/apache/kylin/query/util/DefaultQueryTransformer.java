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

/**
 * DefaultQueryTransformer only used for query from IndexPlan.
 */
public class DefaultQueryTransformer implements IQueryTransformer {

    private static final String S0 = "\\s*";
    private static final String SM = "\\s+";
    private static final String ONE = "1";

    private static final Pattern PTN_SUM = Pattern.compile(
            S0 + "\\bSUM" + S0 + "[(]" + S0 + "(-?\\d+(\\.\\d+)?)" + S0 + "[)]" + S0, Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_INTERVAL = Pattern.compile(
            "\\binterval" + SM + "(floor\\()([\\d.]+)(\\))" + SM + "(second|minute|hour|day|month|year)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PTN_HAVING_ESCAPE_FUNCTION = Pattern
            .compile("[{]" + S0 + "fn" + SM + "(EXTRACT\\(.*?\\))" + S0 + "[}]", Pattern.CASE_INSENSITIVE);

    //TODO #11033
    private static final Pattern PIN_SUM_OF_CAST = Pattern.compile(S0 + "\\bSUM" + S0 + "\\(" + S0 + "CAST" + S0 + "\\("
            + S0 + "([^\\s,]+)" + S0 + "AS" + SM + "DOUBLE" + S0 + "\\)" + S0 + "\\)", Pattern.CASE_INSENSITIVE);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        sql = transformSumOfCast(sql);
        sql = transformEscapeFunction(sql);
        sql = transformSumOfNumericLiteral(sql);
        sql = transformIntervalFunc(sql);
        return sql;
    }

    // Case: SUM(CAST (column_name AS DOUBLE))
    private static String transformSumOfCast(String sql) {
        Matcher m;
        while (true) {
            m = PIN_SUM_OF_CAST.matcher(sql);
            if (!m.find())
                break;

            sql = sql.substring(0, m.start()) + " SUM(" + m.group(1).trim() + ")"
                    + sql.substring(m.end(), sql.length());
        }
        return sql;
    }

    // Case {fn EXTRACT(...) }
    // Use non-greedy regex matching to remove escape functions
    // Notice: Only unsupported escape function need to be handled
    // Reference: https://calcite.apache.org/docs/reference.html#jdbc-function-escape
    private static String transformEscapeFunction(String sql) {
        Matcher m;
        while (true) {
            m = PTN_HAVING_ESCAPE_FUNCTION.matcher(sql);
            if (!m.find())
                break;
            sql = sql.substring(0, m.start()) + m.group(1) + sql.substring(m.end());
        }
        return sql;
    }

    // Case: SUM(numeric_literal) --> numeric_literal * COUNT(1)
    private static String transformSumOfNumericLiteral(String sql) {
        Matcher m;
        while (true) {
            m = PTN_SUM.matcher(sql);
            if (!m.find())
                break;
            String literal = m.group(1);
            String replacedLiteral = ONE.equals(literal) ? " COUNT(1) " : " " + literal + " * COUNT(1) ";
            sql = sql.substring(0, m.start()) + replacedLiteral + sql.substring(m.end());
        }
        return sql;
    }

    // ( date '2001-09-28' + interval floor(1) day ) generated by cognos
    // calcite only recognizes date '2001-09-28' + interval '1' day
    private static String transformIntervalFunc(String sql) {
        Matcher m;
        while (true) {
            m = PTN_INTERVAL.matcher(sql);
            if (!m.find())
                break;

            int value = (int) Math.floor(Double.parseDouble(m.group(2)));
            sql = sql.substring(0, m.start(1)) + "'" + value + "'" + sql.substring(m.end(3));
        }
        return sql;
    }

}
