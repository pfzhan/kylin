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

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Collections2;

import io.kyligence.kap.smart.query.SQLResult;

abstract class AbstractSQLAdviceProposer implements ISQLAdviceProposer {
    private final Cache<String, SQLAdvice> CACHE = CacheBuilder.newBuilder().maximumSize(10).build();

    private static final String MSG_UNSUPPORTED_SQL = "Not Supported SQL.";
    private static final String MSG_UNSUPPORTED_SQL2 = "Non-query expression encountered in illegal context";
    private static final String MSG_SYS_DRIVER_STOPPED = "Unknown error! Please make sure the spark driver is working by running \"bin/spark-client.sh start\"";
    private static final String MSG_CARTESIAN_JOIN = "Cartesian Join is not supported.";

    private static final Pattern PTN_SYNTAX_ERROR = Pattern.compile(
            "(?:At line \\d+, column \\d+|From line \\d+, column \\d+ to line \\d+, column \\d+): ([^\n]+)\nwhile executing SQL: \"(.*)\"",
            Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_COLUMN_MISSING = Pattern
            .compile("Column '([^']+)' not found in (?:any table|table '([^']+)')", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_TABLE_MISSING = Pattern
            .compile("Object '([^']*)' not found( within '([^']*)')?", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_TABLE_CASE_ERR = Pattern.compile(
            "Object '([^']*)' not found within '[^']*'; did you mean '([^']*)'\\?", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_SYNTAX_UNEXPECTED_TOKEN = Pattern.compile(
            "Encountered \"(.*)\" at line (\\d+), column (\\d+). Was expecting one of: .*",
            Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_CALCITE_COMPILE = Pattern.compile("Error while compiling generated Java code: .*",
            Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern PTN_DEFAULT_MESSAGE = Pattern.compile("(.*)\nwhile executing SQL: \".*\"",
            Pattern.MULTILINE | Pattern.DOTALL);

    private static final Pattern PTN_NO_REALIZATION_FOUND = Pattern.compile(
            "No realization found for [^\\s]+:OLAPTableScan\\.OLAP\\.\\[\\]\\(table=\\[([^\\s]+), ([^\\s]+)\\].*",
            Pattern.MULTILINE | Pattern.DOTALL);

    private static final Pattern PTN_ARITHMETIC_ERROR = Pattern.compile("ArithmeticException: (.*)",
            Pattern.MULTILINE | Pattern.DOTALL);

    protected AdviceMessage msg = AdviceMsgPicker.getMsg();

    @Override
    public SQLAdvice propose(SQLResult sqlResult) {
        if (sqlResult == null || sqlResult.getMessage() == null) {
            return null;
        }

        String message = sqlResult.getMessage();
        SQLAdvice result = CACHE.getIfPresent(message);
        if (result != null) {
            return result;
        }

        result = proposeWithMessage(message);
        CACHE.put(message, result);
        return result;
    }

    private SQLAdvice proposeWithMessage(String message) {
        switch (message) {
        case MSG_UNSUPPORTED_SQL:
            return SQLAdvice.build(MSG_UNSUPPORTED_SQL, msg.getBAD_SQL_SUGGEST());
        case MSG_UNSUPPORTED_SQL2:
            return SQLAdvice.build(MSG_UNSUPPORTED_SQL, msg.getBAD_SQL_SUGGEST());
        default:
            break;
        }

        // parse error from calcite
        Matcher m = PTN_SYNTAX_UNEXPECTED_TOKEN.matcher(message);
        if (m.matches()) {
            return SQLAdvice.build(String.format(msg.getUNEXPECTED_TOKEN(), m.group(1), m.group(2), m.group(3)),
                    msg.getBAD_SQL_SUGGEST());
        }

        // syntax error from calcite
        m = PTN_SYNTAX_ERROR.matcher(message);
        if (m.matches()) {
            return proposeSyntaxError(m.group(1));
        }

        m = PTN_CALCITE_COMPILE.matcher(message);
        if (m.matches()) {
            return SQLAdvice.build(String.format(msg.getDEFAULT_REASON(), ""), msg.getDEFAULT_SUGGEST());
        }

        m = PTN_NO_REALIZATION_FOUND.matcher(message);
        if (m.matches()) {
            return SQLAdvice.build(msg.getNO_REALIZATION_FOUND_REASON(),
                    String.format(msg.getNO_REALIZATION_FOUND_SUGGEST(), m.group(1), m.group(2)));
        }

        m = PTN_ARITHMETIC_ERROR.matcher(message);
        if (m.matches()) {
            return SQLAdvice.build(msg.getARITHMETIC_ERROR_REASON() + m.group(1), msg.getARITHMETIC_ERROR_SUGGEST());
        }

        // by default, return origin message
        m = PTN_DEFAULT_MESSAGE.matcher(message);
        if (m.matches()) {
            message = m.group(1);

            switch (message) {
            case MSG_SYS_DRIVER_STOPPED:
                return SQLAdvice.build(msg.getSPARK_DRIVER_NOT_RUNNING_REASON(),
                        msg.getSPARK_DRIVER_NOT_RUNNING_SUGGEST());
            case MSG_CARTESIAN_JOIN:
                return SQLAdvice.build(MSG_CARTESIAN_JOIN, msg.getBAD_SQL_SUGGEST());
            default:
                break;
            }
        }
        return SQLAdvice.build(String.format(msg.getDEFAULT_REASON(), message), msg.getDEFAULT_SUGGEST());
    }

    SQLAdvice proposeSyntaxError(String message) {
        Matcher m = PTN_SYNTAX_TABLE_MISSING.matcher(message);
        if (m.matches()) {
            String tblName = m.group(1);
            if (m.group(3) != null) {
                tblName = m.group(3) + "." + tblName;
            }
            return SQLAdvice.build(String.format(msg.getBAD_SQL_TABLE_NOT_FOUND_REASON(), tblName),
                    String.format(msg.getBAD_SQL_TABLE_NOT_FOUND_SUGGEST(), tblName));
        }

        m = PTN_SYNTAX_TABLE_CASE_ERR.matcher(message);
        if (m.matches()) {
            String actual = m.group(1);
            String expected = m.group(2);
            return SQLAdvice.build(String.format(msg.getBAD_SQL_TABLE_CASE_ERR_REASON(), actual),
                    String.format(msg.getBAD_SQL_TABLE_CASE_ERR_SUGGEST(), actual, expected));
        }

        m = PTN_SYNTAX_COLUMN_MISSING.matcher(message);
        if (m.matches()) {
            String colName = m.group(1);
            String tblName = m.group(2);
            if (tblName == null) {
                return SQLAdvice.build(String.format(msg.getBAD_SQL_COLUMN_NOT_FOUND_REASON(), colName),
                        String.format(msg.getBAD_SQL_COLUMN_NOT_FOUND_SUGGEST(), colName));
            } else {
                return SQLAdvice.build(
                        String.format(msg.getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON(), colName, tblName),
                        String.format(msg.getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGEST(), colName, tblName));
            }
        }

        return SQLAdvice.build(String.format(msg.getBAD_SQL_REASON(), message), msg.getBAD_SQL_SUGGEST());
    }

    protected String formatTblColRefs(Collection<TblColRef> tblColRefs) {
        return Joiner.on(", ").join(Collections2.transform(tblColRefs, new Function<TblColRef, String>() {
            @Nullable
            @Override
            public String apply(@Nullable TblColRef tblColRef) {
                return tblColRef.getTable() + "." + tblColRef.getName();
            }
        }));
    }

    protected String formatFunctionDescs(Collection<FunctionDesc> functionDescs) {
        return Joiner.on(", ").join(Collections2.transform(functionDescs, new Function<FunctionDesc, String>() {
            @Nullable
            @Override
            public String apply(@Nullable FunctionDesc functionDesc) {
                return String.format("%s(%s)", functionDesc.getExpression(),
                        formatTblColRefs(functionDesc.getParameter().getColRefs()));
            }
        }));
    }

    protected String formatJoins(Collection<JoinDesc> joins) {
        return Joiner.on(", ").join(Collections2.transform(joins, new Function<JoinDesc, String>() {
            @Nullable
            @Override
            public String apply(@Nullable JoinDesc input) {
                return input.toString().replace("JoinDesc [", "[");
            }
        }));
    }

    protected String formatTables(Collection<OLAPTableScan> olapTableScans) {
        return Joiner.on(", ").join(Collections2.transform(olapTableScans, new Function<OLAPTableScan, String>() {
            @Nullable
            @Override
            public String apply(@Nullable OLAPTableScan input) {
                return input.getTableName();
            }
        }));
    }
}
