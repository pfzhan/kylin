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

import io.kyligence.kap.query.util.EscapeFunction.FnConversion;

public abstract class EscapeDialect {

    private static final String FN_LENGTH_ALIAS = "CHAR_LENGTH";

    /** Define SQL dialect for different data source **/

    /**
     * CALCITE (CUBE)
     */
    public static final EscapeDialect CALCITE = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT
                    , FnConversion.RIGHT
                    , FnConversion.CURRENT_DATE
                    , FnConversion.CURRENT_TIME
                    , FnConversion.CURRENT_TIMESTAMP
                    , FnConversion.CONVERT
                    , FnConversion.TRIM
                    , FnConversion.PI);

            register(FN_LENGTH_ALIAS, FnConversion.FN_LENGTH);
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
            registerAll(FnConversion.LEFT
                    , FnConversion.RIGHT
                    , FnConversion.CONVERT
                    , FnConversion.LOG
                    , FnConversion.CURRENT_DATE
                    , FnConversion.CURRENT_TIME
                    , FnConversion.CURRENT_TIMESTAMP
                    , FnConversion.WEEK
                    , FnConversion.TRIM);

            register(FN_LENGTH_ALIAS, FnConversion.LENGTH);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.normalFN(functionName, args);
        }
    };

    public static final EscapeDialect HIVE = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT
                    , FnConversion.RIGHT
                    , FnConversion.CONVERT
                    , FnConversion.LOG
                    , FnConversion.CURRENT_DATE
                    , FnConversion.CURRENT_TIME
                    , FnConversion.CURRENT_TIMESTAMP
                    , FnConversion.WEEK
                    , FnConversion.TIMESTAMPADD
                    , FnConversion.TIMESTAMPDIFF);

            register(FN_LENGTH_ALIAS, FnConversion.LENGTH);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.normalFN(functionName, args);
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
