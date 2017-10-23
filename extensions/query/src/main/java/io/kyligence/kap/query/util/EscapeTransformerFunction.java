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

public class EscapeTransformerFunction {

    public static String transform(String functionName, String[] args) {
        switch (functionName.toLowerCase()) {
        /* String functions, names in lower case */
        case "left":
            return leftFN(args);
        case "right":
            return rightFN(args);
        case "ltrim":
            return ltrimFN(args);
        case "rtrim":
            return rtrimFN(args);
        case "length":
            return lengthFN(args);
        case "convert":
            return convertFN(args);
        case "lcase":
            return lcaseFN(args);
        case "ucase":
            return ucaseFN(args);
        case "ifnull":
            return ifnullFN(args);
        case "log":
            return logFN(args);
        case "current_date":
            return currentdateFN(args);
        default:
            return normalFN(functionName, args);
        }

    }

    private static String normalFN(String functionName, String[] args) {
        return new StringBuilder().append(functionName).append('(').append(StringUtils.join(args, ", ")).append(')')
                .toString();
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

    private static String leftFN(String[] args) {
        checkArgs(args, 2);
        String[] newArgs = new String[] { args[0], "1", args[1] };
        return normalFN("SUBSTRING", newArgs);
    }

    private static String rightFN(String[] args) {
        checkArgs(args, 2);
        String origStrRef = args[0];
        Integer rightOffset = Integer.valueOf(args[1]);
        String[] newArgs = new String[] { origStrRef, "CHAR_LENGTH(" + origStrRef + ") - " + (rightOffset - 1),
                "" + rightOffset };
        return normalFN("SUBSTRING", newArgs);
    }

    private static String ltrimFN(String[] args) {
        checkArgs(args, 1);
        String[] newArgs = new String[] { "leading " + args[0] };
        return normalFN("TRIM", newArgs);
    }

    private static String rtrimFN(String[] args) {
        checkArgs(args, 1);
        String[] newArgs = new String[] { "trailing " + args[0] };
        return normalFN("TRIM", newArgs);
    }

    private static String lengthFN(String[] args) {
        checkArgs(args, 1);
        return normalFN("CHAR_LENGTH", args);
    }

    private static String convertFN(String[] args) {
        checkArgs(args, 2);
        String value = args[0];
        String sqlType = args[1].toUpperCase();
        String sqlPrefix = "SQL_";
        if (sqlType.startsWith(sqlPrefix)) {
            sqlType = sqlType.substring(sqlPrefix.length());
        }
        String[] newArgs = new String[] { value + " AS " + sqlType };
        return normalFN("CAST", newArgs);
    }

    private static String lcaseFN(String[] args) {
        checkArgs(args, 1);
        return normalFN("LOWER", args);
    }

    private static String ucaseFN(String[] args) {
        checkArgs(args, 1);
        return normalFN("UPPER", args);
    }
    
    private static String ifnullFN(String[] args) {
        checkArgs(args, 2);
        return normalFN("NULLIF", args);
    }
    
    private static String logFN(String[] args) {
        checkArgs(args, 1);
        return normalFN("LN", args);
    }

    private static String currentdateFN(String[] args) {
        checkArgs(args, 0);
        return "CURRENT_DATE";
    }
    
}
