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

package io.kyligence.kap.query.mask;

import io.kyligence.kap.query.util.EscapeDialect;
import io.kyligence.kap.query.util.EscapeParser;
import io.kyligence.kap.query.util.ParseException;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class MaskUtil {

    private MaskUtil() {
    }

    static Dataset<Row> dFToDFWithIndexedColumns(Dataset<Row> df) {
        String[] indexedColNames = new String[df.columns().length];
        for (int i = 0; i < indexedColNames.length; i++) {
            indexedColNames[i] = df.columns()[i].replaceAll("[`.]", "_") + "_" + i;
        }
        return df.toDF(indexedColNames);
    }

    static List<SqlIdentifier> getCCCols(String ccExpr) {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder().setQuoting(Quoting.BACK_TICK);
        String selectSql = "select " + ccExpr;
        EscapeParser parser = new EscapeParser(EscapeDialect.CALCITE, selectSql);
        try {
            selectSql = parser.Input();
        } catch (ParseException e) {
            throw new KylinException(QueryErrorCode.FAILED_PARSE_ERROR, "Failed to convert column expr " + ccExpr, e);
        }
        SqlParser sqlParser = SqlParser.create(selectSql, parserBuilder.build());
        SqlSelect select;
        try {
            select = (SqlSelect) sqlParser.parseQuery();
        } catch (SqlParseException e) {
            throw new KylinException(QueryErrorCode.FAILED_PARSE_ERROR, "Failed to parse computed column expr " + ccExpr, e);
        }
        return select.getSelectList().getList().stream().flatMap(op -> getSqlIdentifiers(op).stream()).collect(Collectors.toList());
    }

    static List<SqlIdentifier> getSqlIdentifiers(SqlNode sqlNode) {
        List<SqlIdentifier> ids = new ArrayList<>();
        if (sqlNode instanceof SqlIdentifier) {
            ids.add((SqlIdentifier) sqlNode);
            return ids;
        } else if (sqlNode instanceof SqlCall) {
            return ((SqlCall) sqlNode).getOperandList().stream().flatMap(op -> getSqlIdentifiers(op).stream()).collect(Collectors.toList());
        } else {
            return ids;
        }
    }
}
