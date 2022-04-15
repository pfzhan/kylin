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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Locale;

import io.kyligence.kap.query.engine.PrepareSqlStateParam;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigCannotInitException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.rest.exception.InternalErrorException;

public class PrepareSQLUtils {

    private static final char LITERAL_QUOTE = '\'';
    private static final char PARAM_PLACEHOLDER = '?';

    private PrepareSQLUtils() {
    }

    public static String fillInParams(String prepareSQL, PrepareSqlStateParam[] params) {

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
            throw new IllegalStateException(String.format(Locale.ROOT,
                    "Invalid PrepareStatement, failed to match params with place holders, sql: %s, params: %s",
                    prepareSQL, Arrays.stream(params).map(PrepareSqlStateParam::getValue)));
        }
        return prepareSQL;
    }

    private static String convertToLiteralString(PrepareSqlStateParam param) {
        Object value = getValue(param);
        if (value == null) {
            return "NULL";
        }

        if (value instanceof String) {
            if (param.getClassName().equals(BigDecimal.class.getCanonicalName())) {
                return (String) value;
            }
            return LITERAL_QUOTE + (String) value + LITERAL_QUOTE;
        } else if (value instanceof java.sql.Date) {
            return String.format(Locale.ROOT, "date'%s'", DateFormat.formatToDateStr(((Date) value).getTime()));
        } else if (value instanceof Timestamp) {
            return String.format(Locale.ROOT, "timestamp'%s'", DateFormat.formatToTimeStr(((Timestamp) value).getTime()));
        } else {
            return String.valueOf(value); // numbers
        }
    }

    private static Object getValue(PrepareSqlStateParam param) {
        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException(e);
        }
        ColumnMetaData.Rep type = ColumnMetaData.Rep.of(clazz);

        String value = param.getValue();
        boolean isNull = (null == value);
        if (isNull || value.isEmpty()) {
            return getEmptyValue(type, isNull);
        }
        return getTypedValue(type, value);
    }

    private static Object getEmptyValue(ColumnMetaData.Rep type, boolean isNull) {
        switch (type) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            return isNull ? null : "";
        case PRIMITIVE_INT:
        case INTEGER:
            return 0;
        case PRIMITIVE_SHORT:
        case SHORT:
            return (short) 0;
        case PRIMITIVE_LONG:
        case LONG:
            return (long) 0;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            return (float) 0;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            return (double) 0;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            return false;
        case PRIMITIVE_BYTE:
        case BYTE:
            return (byte) 0;
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
        case JAVA_SQL_TIME:
        case JAVA_SQL_TIMESTAMP:
        default:
            return null;
        }
    }

    private static Object getTypedValue(ColumnMetaData.Rep type, String value) {
        switch (type) {
        case PRIMITIVE_INT:
        case INTEGER:
            return Integer.parseInt(value);
        case PRIMITIVE_SHORT:
        case SHORT:
            return Short.parseShort(value);
        case PRIMITIVE_LONG:
        case LONG:
            return Long.parseLong(value);
        case PRIMITIVE_FLOAT:
        case FLOAT:
            return Float.parseFloat(value);
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            return Double.parseDouble(value);
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            return Boolean.parseBoolean(value);
        case PRIMITIVE_BYTE:
        case BYTE:
            return Byte.parseByte(value);
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
            return java.sql.Date.valueOf(value);
        case JAVA_SQL_TIME:
            return Time.valueOf(value);
        case JAVA_SQL_TIMESTAMP:
            return Timestamp.valueOf(value);
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
        default:
            return value;
        }
    }

    private static int findNextPlaceHolder(String prepareSQL, int start) {
        boolean openingIdentQuote = false;
        boolean openingLiteralQuote = false;
        while (start < prepareSQL.length()) {
            if (prepareSQL.charAt(start) == LITERAL_QUOTE) {
                // skip quoted literal
                openingLiteralQuote = !openingLiteralQuote;
            } else if (!openingLiteralQuote && prepareSQL.charAt(start) == identQuoting()) {
                // skip quoted identifier
                openingIdentQuote = !openingIdentQuote;
            }
            if (openingLiteralQuote || openingIdentQuote) {
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
