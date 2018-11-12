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

import org.apache.commons.lang3.StringUtils;

public class EscapeFunction {

    /* Function conversion implementations */
    /*
     * Notice: Prefix "FN_" means converting to {fn ...} format
     */
    public enum FnConversion {
        LEFT(args -> {
            checkArgs(args, 2);
            String[] newArgs = new String[] { args[0], "1", args[1] };
            return normalFN("SUBSTRING", newArgs);
        }),
        RIGHT(args -> {
            checkArgs(args, 2);
            String origStrRef = args[0];
            String rightOffset = args[1];
            String[] newArgs = new String[] { origStrRef, "CHAR_LENGTH(" + origStrRef + ") + 1 - " + rightOffset,
                    "" + rightOffset };
            return normalFN("SUBSTRING", newArgs);
        }),
        TRIM(args -> {
            checkArgs(args, 1);
            String[] newArgs = new String[] { "both " + args[0] };
            return normalFN("TRIM", newArgs);
        }
        ),
        LTRIM(args -> {
            checkArgs(args, 1);
            String[] newArgs = new String[] { "leading " + args[0] };
            return normalFN("TRIM", newArgs);
        }),
        RTRIM(args -> {
            checkArgs(args, 1);
            String[] newArgs = new String[] { "trailing " + args[0] };
            return normalFN("TRIM", newArgs);
        }),
        FN_LENGTH(args -> {
            checkArgs(args, 1);
            return scalarFN("LENGTH", args);
        }),
        LENGTH(args -> {
            checkArgs(args, 1);
            return normalFN("LENGTH", args);
        }),
        CONVERT(args -> {
            checkArgs(args, 2);
            String value = args[0];
            String sqlType = args[1].toUpperCase();
            String sqlPrefix = "SQL_";
            if (sqlType.startsWith(sqlPrefix)) {
                sqlType = sqlType.substring(sqlPrefix.length());
            }
            switch (sqlType) {
                case "WVARCHAR":
                case "CHAR":
                case "WCHAR":
                    sqlType = "VARCHAR";
                    break;
                default:
                    break;
            }
            String[] newArgs = new String[] { value + " AS " + sqlType };
            return normalFN("CAST", newArgs);
        }),
        LCASE(args -> {
            checkArgs(args, 1);
            return normalFN("LOWER", args);
        }),
        UCASE(args -> {
            checkArgs(args, 1);
            return normalFN("UPPER", args);
        }),
        LOG(args -> {
            checkArgs(args, 1);
            return normalFN("LN", args);
        }),
        PI(args -> {
            checkArgs(args, 0);
            return "PI";
        }),
        CURRENT_DATE(args -> {
            checkArgs(args, 0);
            return "CURRENT_DATE";
        }),
        CURRENT_TIME(args -> {
            checkArgs(args, 0);
            return "CURRENT_TIME";
        }),
        CURRENT_TIMESTAMP(args -> {
            if (args == null || args.length > 1) {
                throw new IllegalArgumentException("Bad arguments");
            }
            return "CURRENT_TIMESTAMP";
        }),
        WEEK(args -> {
            checkArgs(args, 1);
            return normalFN("WEEKOFYEAR", args);
        }),
        TIMESTAMPADD(args -> {
            checkArgs(args, 3);
            String[] newArgs = {"'" + args[0] + "'", args[1], args[2]};
            return normalFN("TIMESTAMPADD", newArgs);
        }),
        TIMESTAMPDIFF(args -> {
            checkArgs(args, 3);
            String[] newArgs = {"'" + args[0] + "'", args[1], args[2]};
            return normalFN("TIMESTAMPDIFF", newArgs);
        });

        private interface IConvert {
            public String convert(String[] args);
        }

        private IConvert innerCvt;

        FnConversion(IConvert convert) {
            this.innerCvt = convert;
        }

        public String convert(String[] args) {
            return innerCvt.convert(args);
        }
    }

    // Present as normal function: "func(arg1, arg2, ...)"
    public static String normalFN(String functionName, String[] args) {
        return new StringBuilder().append(functionName).append('(').append(StringUtils.join(args, ", ")).append(')')
                .toString();
    }

    // Present as JDBC/ODBC scalar function: "{ fn func(arg1, arg2, ...) }"
    public static String scalarFN(String functionName, String[] args) {
        return new StringBuilder().append("{fn ").append(functionName).append('(').append(StringUtils.join(args, ", "))
                .append(")}").toString();
    }

    private static void checkArgs(String[] args, int expectedCount) {
        int actualCount = 0;
        if (args != null) {
            actualCount = args.length;
        }
        if (actualCount != expectedCount) {
            throw new IllegalArgumentException(
                    "Bad arguments count, expected count " + expectedCount + " but actual " + actualCount);
        }
    }
}
