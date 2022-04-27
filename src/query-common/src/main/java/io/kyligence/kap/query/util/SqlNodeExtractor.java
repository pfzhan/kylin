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

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SqlNodeExtractor extends SqlBasicVisitor<SqlNode> {

    private List<SqlIdentifier> allSqlIdentifier = Lists.newArrayList();

    public static List<SqlIdentifier> getAllSqlIdentifier(String sql) throws SqlParseException {
        SqlNode parsed = CalciteParser.parse(sql);
        SqlNodeExtractor sqlNodeExtractor = new SqlNodeExtractor();
        parsed.accept(sqlNodeExtractor);
        return sqlNodeExtractor.allSqlIdentifier;
    }

    public static Map<SqlIdentifier, Pair<Integer, Integer>> getIdentifierPos(String sql) throws SqlParseException {
        List<SqlIdentifier> identifiers = getAllSqlIdentifier(sql);
        Map<SqlIdentifier, Pair<Integer, Integer>> identifierAndPositionMap = Maps.newHashMap();
        for (SqlIdentifier identifier : identifiers) {
            Pair<Integer, Integer> identifyPosition = CalciteParser.getReplacePos(identifier, sql);
            identifierAndPositionMap.put(identifier, identifyPosition);
        }
        return identifierAndPositionMap;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        allSqlIdentifier.add(id);
        return null;
    }
}
