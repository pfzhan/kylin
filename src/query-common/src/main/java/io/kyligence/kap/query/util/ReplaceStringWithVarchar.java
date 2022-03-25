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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceStringWithVarchar implements KapQueryUtil.IQueryTransformer {
    private static final Logger logger = LoggerFactory.getLogger(ReplaceStringWithVarchar.class);
    private static final String REPLACED = "STRING";
    private static final String REPLACER = "VARCHAR";

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        try {
            SqlNode sqlNode = CalciteParser.parse(sql, project);
            SqlStringTypeCapturer visitor = new SqlStringTypeCapturer();
            sqlNode.accept(visitor);
            List<SqlNode> stringTypeNodes = visitor.getStringTypeNodes();
            String result = sql;
            for (SqlNode node : stringTypeNodes) {
                    result = replaceStringWithVarchar(node, result);

            }
            return result;
        } catch (Exception e) {
            logger.error("replace `STRING` with `VARCHAR` error: ", e);
            return sql;
        }
    }

    private String replaceStringWithVarchar(SqlNode sqlNode, String sql) {
        Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(sqlNode, sql);
        StringBuilder result = new StringBuilder(sql);
        result.replace(startEndPos.getFirst(), startEndPos.getSecond(), REPLACER);
        return result.toString();
    }

    static class SqlStringTypeCapturer extends SqlBasicVisitor<SqlNode> {
        List<SqlNode> stringTypeNodes = new ArrayList<>();

        @Override
        public SqlNode visit(SqlDataTypeSpec sqlDataTypeSpec) {
            String names = sqlDataTypeSpec.getTypeName().names.get(0);
            if (REPLACED.equalsIgnoreCase(names))
                stringTypeNodes.add(sqlDataTypeSpec);
            return null;
        }

        public List<SqlNode> getStringTypeNodes() {
            stringTypeNodes.sort((o1, o2) -> {
                SqlParserPos pos1 = o1.getParserPosition();
                SqlParserPos pos2 = o2.getParserPosition();
                int line1 = pos1.getLineNum();
                int line2 = pos2.getLineNum();
                int col1 = pos1.getColumnNum();
                int col2 = pos2.getColumnNum();
                if (line1 > line2)
                    return -1;
                else if (line1 < line2)
                    return 1;
                else
                    return col2 - col1;
            });
            return stringTypeNodes;
        }
    }
}
