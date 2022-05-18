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

package io.kyligence.kap.smart.query.advisor;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;

import io.kyligence.kap.smart.query.SQLResult;

public abstract class AbstractSqlAdvisor implements ISqlAdvisor {
    private static final String MSG_UNSUPPORTED_SQL = "Not Supported SQL.";
    private static final String MSG_UNSUPPORTED_SQL2 = "Non-query expression encountered in illegal context";

    private static final Pattern PTN_SYNTAX_ERROR = Pattern.compile(
            "(?:At line \\d+, column \\d+|From line \\d+, column \\d+ to line \\d+, column \\d+): ([^\n]+)\nwhile executing SQL: \"(.*)\"",
            Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_COLUMN_MISSING = Pattern
            .compile("Column '([^']+)' not found in (?:any table|table '([^']+)')", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_TABLE_MISSING = Pattern
            .compile("Object '([^']*)' not found( within '([^']*)')?", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_UNEXPECTED_TOKEN = Pattern.compile(
            "Encountered \"(.*)\" at line (\\d+), column (\\d+). Was expecting one of: .*",
            Pattern.MULTILINE | Pattern.DOTALL);

    public SQLAdvice proposeWithMessage(SQLResult sqlResult) {
        if (sqlResult == null || sqlResult.getMessage() == null) {
            return null;
        }

        String message = sqlResult.getMessage();
        return proposeWithMessage(message);
    }

    private SQLAdvice proposeWithMessage(String message) {
        switch (message) {
        case MSG_UNSUPPORTED_SQL:
        case MSG_UNSUPPORTED_SQL2:
            return SQLAdvice.build(MSG_UNSUPPORTED_SQL, MsgPicker.getMsg().getBadSqlSuggest());
        default:
            break;
        }

        // parse error from calcite
        Matcher m = PTN_SYNTAX_UNEXPECTED_TOKEN.matcher(message);
        if (m.matches()) {
            return SQLAdvice.build(String.format(Locale.ROOT, MsgPicker.getMsg().getUnexpectedToken(), m.group(1),
                    m.group(2), m.group(3)), MsgPicker.getMsg().getBadSqlSuggest());
        }

        // syntax error from calcite
        m = PTN_SYNTAX_ERROR.matcher(message);
        if (m.matches()) {
            return proposeSyntaxError(m.group(1));
        }

        return SQLAdvice.build(String.format(Locale.ROOT, MsgPicker.getMsg().getDefaultReason(), message),
                MsgPicker.getMsg().getDefaultSuggest());
    }

    private SQLAdvice proposeSyntaxError(String message) {
        Matcher m = PTN_SYNTAX_TABLE_MISSING.matcher(message);
        if (m.matches()) {
            String tblName = m.group(1);
            if (m.group(3) != null) {
                tblName = m.group(3) + "." + tblName;
            }
            return SQLAdvice.build(
                    String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlTableNotFoundReason(), tblName),
                    String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlTableNotFoundSuggest(), tblName));
        }

        m = PTN_SYNTAX_COLUMN_MISSING.matcher(message);
        if (m.matches()) {
            String colName = m.group(1);
            String tblName = m.group(2);
            if (tblName == null) {
                return SQLAdvice.build(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlColumnNotFoundReason(), colName),
                        String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlColumnNotFoundSuggest(), colName));
            } else {
                return SQLAdvice.build(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlColumnNotFoundInTableReason(), colName),
                        String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlColumnNotFoundInTableSuggestion(),
                                colName));
            }
        }

        return SQLAdvice.build(String.format(Locale.ROOT, MsgPicker.getMsg().getBadSqlReason(), message),
                MsgPicker.getMsg().getBadSqlSuggest());
    }

    SQLAdvice adviseSyntaxError(SQLResult sqlResult) {
        if (sqlResult.getException() != null && !(sqlResult.getException() instanceof NoRealizationFoundException)
                && !(sqlResult.getException().getCause() instanceof NoRealizationFoundException)) {
            return proposeWithMessage(sqlResult);
        }
        return null;
    }
}
