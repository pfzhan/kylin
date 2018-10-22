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

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

public class ExpressionComparatorTest {

    @Test
    public void testBasicEqual() throws SqlParseException {
        String sql0 = "select a.a + a.b + a.c from t as a";
        String sql1 = "select (((a . a +    a.b +    a.c))) from t as a";
        String sql2 = "select a.a + (a.b + a.c) from t as a";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);
        SqlNode sn3 = CalciteParser.getOnlySelectNode(sql2);

        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("A", "A");
        QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, null);

        assertEquals(true, ExpressionComparator.isNodeEqual(sn0, sn1, matchInfo, IAliasDeduce.NO_OP));
        assertEquals(false, ExpressionComparator.isNodeEqual(sn0, sn3, matchInfo, IAliasDeduce.NO_OP));

    }

    @Test
    public void testAdvancedEqual() throws SqlParseException {
        //treat sql0 as model
        String sql0 = "select a.a + a.b + a.c from t as a";

        String sql1 = "select b.a + b.b + b.c from t as b";
        String sql2 = "select (a + b) + c  from t";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);
        SqlNode sn2 = CalciteParser.getOnlySelectNode(sql2);

        // when query using different alias than model
        {
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.put("B", "A");

            ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("T", "B", //
                    Pair.newPair("A", "integer"), //
                    Pair.newPair("B", "integer"), //
                    Pair.newPair("C", "integer"));

            LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
            mockQueryAlias.put("B", columnRowType);

            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);
            assertEquals(true, ExpressionComparator.isNodeEqual(sn1, sn0, matchInfo, IAliasDeduce.NO_OP));
        }

        // when query not using alias
        {
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.put("T", "A");

            ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("T", "T", //
                    Pair.newPair("A", "integer"), //
                    Pair.newPair("B", "integer"), //
                    Pair.newPair("C", "integer"));

            LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
            mockQueryAlias.put("T", columnRowType);

            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);
            assertEquals(true, ExpressionComparator.isNodeEqual(sn2, sn0, matchInfo, new AliasDeduceImpl(matchInfo)));
        }

    }
}
