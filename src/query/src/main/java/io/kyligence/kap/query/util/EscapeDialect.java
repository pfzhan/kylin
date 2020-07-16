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

package io.kyligence.kap.query.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.avatica.util.Quoting;

import io.kyligence.kap.query.util.EscapeFunction.FnConversion;

public abstract class EscapeDialect {

    private static final String FN_LENGTH_ALIAS = "CHAR_LENGTH";
    private static final String FN_WEEK = "WEEK";
    private static final String FN_CEIL = "CEIL";
    private static final String FN_FLOOR = "FLOOR";
    private static final String FN_SUBSTR = "SUBSTR";
    private static final String FN_SUBSTRING = "SUBSTRING";

    /* Define SQL dialect for different data source */

    /**
     * CALCITE (CUBE)
     */
    public static final EscapeDialect CALCITE = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT, //
                    FnConversion.RIGHT, //
                    FnConversion.CURRENT_DATE, //
                    FnConversion.CURRENT_TIME, //
                    FnConversion.CURRENT_TIMESTAMP, //
                    FnConversion.CONVERT, //
                    FnConversion.TRIM, //
                    FnConversion.TIMESTAMPADD, //
                    FnConversion.TIMESTAMPDIFF, //
                    FnConversion.YEAR, //
                    FnConversion.QUARTER, //
                    FnConversion.MONTH, //
                    FnConversion.DAYOFMONTH, //
                    FnConversion.DAYOFYEAR, //
                    FnConversion.DAYOFWEEK, //
                    FnConversion.HOUR, //
                    FnConversion.MINUTE, //
                    FnConversion.SECOND, //
                    FnConversion.PI);

            register(FN_LENGTH_ALIAS, FnConversion.FN_LENGTH);
            register(FN_WEEK, FnConversion.WEEK_CALCITE);
            register(FN_CEIL, FnConversion.CEIL);
            register(FN_FLOOR, FnConversion.FLOOR);
            register(FN_SUBSTR, FnConversion.SUSTR);
            register(FN_SUBSTRING, FnConversion.SUSTRING);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.scalarFN(functionName, args);
        }
    };

    /**
     * SPARK SQL
     */
    public static final EscapeDialect SPARK_SQL = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT, //
                    FnConversion.RIGHT, //
                    FnConversion.CONVERT, //
                    FnConversion.LOG, //
                    FnConversion.CURRENT_DATE, //
                    FnConversion.CURRENT_TIME, //
                    FnConversion.CURRENT_TIMESTAMP, //
                    FnConversion.TIMESTAMPADD, //
                    FnConversion.TIMESTAMPDIFF, //
                    FnConversion.YEAR, //
                    FnConversion.QUARTER, //
                    FnConversion.MONTH, //
                    FnConversion.DAYOFMONTH, //
                    FnConversion.DAYOFYEAR, //
                    FnConversion.DAYOFWEEK, //
                    FnConversion.HOUR, //
                    FnConversion.MINUTE, //
                    FnConversion.SECOND, //
                    FnConversion.TRIM //
            );

            register(FN_WEEK, FnConversion.WEEK_SPARK);
            register(FN_LENGTH_ALIAS, FnConversion.LENGTH);
            register(FN_CEIL, FnConversion.CEIL2);
            register(FN_FLOOR, FnConversion.FLOOR2);
            register(FN_SUBSTR, FnConversion.SUSTR);
            register(FN_SUBSTRING, FnConversion.SUSTRING);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.normalFN(functionName, args);
        }

        @Override
        public String transformDoubleQuoteString(String input) {
            if (!input.startsWith("\"")) {
                return input;
            }
            return Quoting.BACK_TICK.string + input.substring(1, input.length() - 1) + Quoting.BACK_TICK.string;
        }

        @Override
        public String transformTimeunitOfTimestampDiffOrAdd(String timeunit) {
            return "'" + timeunit + "'";
        }

        @Override
        public String transformDataType(String type) {
            if (type.equalsIgnoreCase("INTEGER")) {
                return type.substring(0, 3);
            } else if (type.equalsIgnoreCase("WVARCHAR")) {
                return type.substring(1);
            } else if (type.equalsIgnoreCase("SQL_WVARCHAR")) {
                return type.substring(5);
            }
            return type;
        }
    };

    public static final EscapeDialect HIVE = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT, //
                    FnConversion.RIGHT, //
                    FnConversion.CONVERT, //
                    FnConversion.LOG, //
                    FnConversion.CURRENT_DATE, //
                    FnConversion.CURRENT_TIME, //
                    FnConversion.CURRENT_TIMESTAMP, //
                    FnConversion.TIMESTAMPADD, //
                    FnConversion.TIMESTAMPDIFF, //
                    FnConversion.YEAR, //
                    FnConversion.QUARTER, //
                    FnConversion.MONTH, //
                    FnConversion.DAYOFMONTH, //
                    FnConversion.DAYOFYEAR, //
                    FnConversion.DAYOFWEEK, //
                    FnConversion.HOUR, //
                    FnConversion.MINUTE, //
                    FnConversion.SECOND //
            );

            register(FN_LENGTH_ALIAS, FnConversion.LENGTH);
            register(FN_WEEK, FnConversion.WEEK_SPARK);
            register("TRUNCATE", FnConversion.TRUNCATE_NUM);
            register(FN_CEIL, FnConversion.CEIL2);
            register(FN_FLOOR, FnConversion.FLOOR2);
            register(FN_SUBSTR, FnConversion.SUSTR);
            register(FN_SUBSTRING, FnConversion.SUSTRING);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.normalFN(functionName, args);
        }

        @Override
        public String transformDoubleQuoteString(String input) {
            if (!input.startsWith("\"")) {
                return input;
            }
            return Quoting.BACK_TICK.string + input.substring(1, input.length() - 1) + Quoting.BACK_TICK.string;
        }

        @Override
        public String transformTimeunitOfTimestampDiffOrAdd(String timeunit) {
            return "'" + timeunit + "'";
        }
    };

    public static final EscapeDialect DEFAULT = CALCITE; // Default dialect is CALCITE

    /**
     * base of function dialects
     */

    private Map<String, FnConversion> registeredFunction = new HashMap<>();

    public EscapeDialect() {
        init();
    }

    public abstract void init();

    public abstract String defaultConversion(String functionName, String[] args);

    public String transformDoubleQuoteString(String input) {
        return input;
    }

    public String transformTimeunitOfTimestampDiffOrAdd(String timeunit) {
        return timeunit;
    }

    public String transformDataType(String type) {
        return type;
    }

    public String transformFN(String functionName, String[] args) {
        FnConversion fnType = registeredFunction.get(functionName.toUpperCase());
        if (fnType != null) {
            return fnType.convert(args);
        } else {
            return defaultConversion(functionName, args);
        }
    }

    public void registerAll(FnConversion... fnTypes) {
        Arrays.stream(fnTypes).forEach(this::register);
    }

    public void register(FnConversion fnType) {
        register(fnType.name(), fnType);
    }

    // Support register function with different names
    public void register(String fnAlias, FnConversion fnType) {
        if (fnType == null) {
            return;
        }
        registeredFunction.put(fnAlias, fnType);
    }
}
