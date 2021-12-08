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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.alias.AliasMapping;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;

public class ExpressionComparatorTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

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

        assertEquals(true, ExpressionComparator.isNodeEqual(sn0, sn1, matchInfo, AliasDeduceImpl.NO_OP));
        assertEquals(false, ExpressionComparator.isNodeEqual(sn0, sn3, matchInfo, AliasDeduceImpl.NO_OP));

    }

    @Test
    public void testCommutativeEqual() throws SqlParseException {
        String sql0 = "select a.a + a.b * a.c from t as a";
        String sql1 = "select a.c * a.b + a.a from t as a";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);

        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("A", "A");
        QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, null);

        Assert.assertTrue(ExpressionComparator.isNodeEqual(sn0, sn1, matchInfo, AliasDeduceImpl.NO_OP));
    }

    @Test
    public void testAdvancedEqual() throws SqlParseException {
        //treat sql0 as model
        String sql0 = "select a.a + a.b + a.c, cast(a.d as decimal(19,4)) from t as a";

        String sql1 = "select b.a + b.b + b.c, cast(b.d as decimal(19,4)) from t as b";
        String sql2 = "select (a + b) + c, cast(d as decimal(19,4)) from t";

        SqlNode sn0 = CalciteParser.getSelectNode(sql0);
        SqlNode sn1 = CalciteParser.getSelectNode(sql1);
        SqlNode sn2 = CalciteParser.getSelectNode(sql2);

        // when query using different alias than model
        {
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.put("B", "A");

            ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("T", "B",
                    ImmutableList.of(Pair.newPair("A", "integer"), //
                            Pair.newPair("B", "integer"), //
                            Pair.newPair("C", "integer"), //
                            Pair.newPair("D", "integer")));

            LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
            mockQueryAlias.put("B", columnRowType);

            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);
            assertEquals(true, ExpressionComparator.isNodeEqual(sn1, sn0, matchInfo, AliasDeduceImpl.NO_OP));
        }

        // when query not using alias
        {
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.put("T", "A");

            ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("T", "T", //
                    ImmutableList.of(Pair.newPair("A", "integer"), //
                            Pair.newPair("B", "integer"), //
                            Pair.newPair("C", "integer"), //
                            Pair.newPair("D", "integer")));

            LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
            mockQueryAlias.put("T", columnRowType);

            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);
            assertEquals(true, ExpressionComparator.isNodeEqual(sn2, sn0, matchInfo, new AliasDeduceImpl(matchInfo)));
        }

    }

    @Test
    public void testNoNPE() {
        //https://github.com/Kyligence/KAP/issues/10934
        String sql0 = "select a.a + a.b + a.c from t as a";
        String sql1 = "select a.a + a.b + a.c from t as a";
        String sql2 = "select 1";
        String sql3 = "select 1";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);
        SqlNode sn2 = CalciteParser.getOnlySelectNode(sql2);
        SqlNode sn3 = CalciteParser.getOnlySelectNode(sql3);
        {
            AliasMapping aliasMapping = null;
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);

            assertEquals(false, matchInfo.isSqlNodeEqual(sn0, sn1));
        }
        {
            AliasMapping aliasMapping = new AliasMapping(null);
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);
            assertEquals(false, matchInfo.isSqlNodeEqual(sn0, sn1));
        }
        {
            AliasMapping aliasMapping = null;
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);
            assertEquals(true, matchInfo.isSqlNodeEqual(sn2, sn3));
        }
        {
            AliasMapping aliasMapping = new AliasMapping(null);
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);
            assertEquals(true, matchInfo.isSqlNodeEqual(sn2, sn3));
        }

    }
}
