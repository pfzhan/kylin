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

package org.apache.kylin.rest.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigCannotInitException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.PrepareSqlRequest;

public class PrepareSQLUtils {

    private static final char LITERAL_QUOTE = '\'';
    private static final char PARAM_PLACEHOLDER = '?';

    public static String fillInParams(String prepareSQL, PrepareSqlRequest.StateParam[] params) {

        int startOffset = 0;
        int placeHolderIdx = -1;
        int paramIdx = 0;

        placeHolderIdx = findNextPlaceHolder(prepareSQL, startOffset);
        while (placeHolderIdx != -1 && paramIdx < params.length) {
            String paramLiteral = convertToLiteralString(params[paramIdx]);
            prepareSQL = prepareSQL.substring(0, placeHolderIdx) + paramLiteral + prepareSQL.substring(placeHolderIdx + 1);

            paramIdx += 1;
            startOffset = placeHolderIdx + paramLiteral.length();
            placeHolderIdx = findNextPlaceHolder(prepareSQL, startOffset);
        }

        if (paramIdx != params.length) {
            throw new IllegalStateException(String.format(
                    "Invalid PrepareStatement, failed to match params with place holders, sql: %s, params: %s",
                    prepareSQL, Arrays.stream(params).map(PrepareSqlRequest.StateParam::getValue)));
        }
        return prepareSQL;
    }

    private static String convertToLiteralString(PrepareSqlRequest.StateParam param) {
        Object value = getValue(param);
        if (value == null) {
            return "NULL";
        }

        if (value instanceof String) {
            return LITERAL_QUOTE + (String) value + LITERAL_QUOTE;
        } else if (value instanceof java.sql.Date) {
            return String.format("date'%s'", DateFormat.formatToDateStr(((Date) value).getTime()));
        } else if (value instanceof Timestamp) {
            return String.format("timestamp'%s'", DateFormat.formatToTimeStr(((Timestamp) value).getTime()));
        } else {
            return String.valueOf(value); // numbers
        }
    }

    private static Object getValue(PrepareSqlRequest.StateParam param) {
        boolean isNull = (null == param.getValue());

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException(e);
        }

        ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);

        switch (rep) {
            case PRIMITIVE_CHAR:
            case CHARACTER:
            case STRING:
                return isNull ? null : param.getValue();
            case PRIMITIVE_INT:
            case INTEGER:
                return isNull ? 0 : Integer.parseInt(param.getValue());
            case PRIMITIVE_SHORT:
            case SHORT:
                return isNull ? 0: Short.valueOf(param.getValue());
            case PRIMITIVE_LONG:
            case LONG:
                return isNull ? 0: Long.parseLong(param.getValue());
            case PRIMITIVE_FLOAT:
            case FLOAT:
                return isNull ? 0: Float.parseFloat(param.getValue());
            case PRIMITIVE_DOUBLE:
            case DOUBLE:
                return isNull ? 0 : Double.parseDouble(param.getValue());
            case PRIMITIVE_BOOLEAN:
            case BOOLEAN:
                return !isNull && Boolean.parseBoolean(param.getValue());
            case PRIMITIVE_BYTE:
            case BYTE:
                return isNull ? 0 : Byte.valueOf(param.getValue());
            case JAVA_UTIL_DATE:
            case JAVA_SQL_DATE:
                return isNull ? null : java.sql.Date.valueOf(param.getValue());
            case JAVA_SQL_TIME:
                return isNull ? null : Time.valueOf(param.getValue());
            case JAVA_SQL_TIMESTAMP:
                return isNull ? null : Timestamp.valueOf(param.getValue());
            default:
                return isNull ? null : param.getValue();
        }
    }

    private static int findNextPlaceHolder(String prepareSQL, int start) {
        boolean openingIdentQuote = false;
        boolean openingLiteralQuote = false;
        while (start < prepareSQL.length()) {
            // skip quoted literal
            if (prepareSQL.charAt(start) == LITERAL_QUOTE) {
                openingLiteralQuote = !openingLiteralQuote;
            }
            if (openingLiteralQuote) {
                start++;
                continue;
            }

            // skip quoted identifier
            if (prepareSQL.charAt(start) == identQuoting()) {
                openingIdentQuote = !openingIdentQuote;
            }
            if (openingIdentQuote) {
                start++;
                continue;
            }

            if (prepareSQL.charAt(start) == PARAM_PLACEHOLDER) {
                return start;
            }

            start++;
        }

        return -1;
    }

    private static char identQuoting() {
        KylinConfig kylinConfig;
        try {
            kylinConfig = KylinConfig.getInstanceFromEnv();
        } catch (KylinConfigCannotInitException e) {
            return Quoting.DOUBLE_QUOTE.string.charAt(0);
        }

        String quoting = kylinConfig.getCalciteExtrasProperties().get("quoting");
        if (quoting != null) {
            return Quoting.valueOf(quoting).string.charAt(0);
        }
        String lex = kylinConfig.getCalciteExtrasProperties().get("lex");
        if (lex != null) {
            return Lex.valueOf(lex).quoting.string.charAt(0);
        }
        return Quoting.DOUBLE_QUOTE.string.charAt(0);
    }
}
