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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.model.tool;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Preconditions;

public class CalciteParserTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testNoTableNameExists() throws SqlParseException {
        String expr1 = "a + b";
        assertEquals("x.a + x.b", CalciteParser.insertAliasInExpr(expr1, "x"));

        String expr2 = "a + year(b)";
        assertEquals("x.a + year(x.b)", CalciteParser.insertAliasInExpr(expr2, "x"));

        String expr3 = "a + hiveudf(b)";
        assertEquals("x.a + hiveudf(x.b)", CalciteParser.insertAliasInExpr(expr3, "x"));
    }

    @Test
    public void testTableNameExists1() throws SqlParseException {
        String expr1 = "a + x.b";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("SqlIdentifier X.B contains DB/Table name");
        CalciteParser.insertAliasInExpr(expr1, "x");
    }

    @Test
    public void testTableNameExists2() throws SqlParseException {
        String expr1 = "a + year(x.b)";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("SqlIdentifier X.B contains DB/Table name");
        CalciteParser.insertAliasInExpr(expr1, "x");
    }

    @Test
    public void testTableNameExists3() throws SqlParseException {
        String expr1 = "a + hiveudf(x.b)";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("SqlIdentifier X.B contains DB/Table name");
        CalciteParser.insertAliasInExpr(expr1, "x");
    }

    @Test
    public void testCasewhen() {
        String expr = "(CASE LSTG_FORMAT_NAME  WHEN 'Auction' THEN 'x'  WHEN 'y' THEN '222' ELSE 'z' END)";
        String alias = "TEST_KYLIN_FACT";
        String s = CalciteParser.insertAliasInExpr(expr, alias);
        System.out.println(s);
        assertEquals(
                "(CASE TEST_KYLIN_FACT.LSTG_FORMAT_NAME  WHEN 'Auction' THEN 'x'  WHEN 'y' THEN '222' ELSE 'z' END)",
                s);
    }

    @Test
    public void testPos() throws SqlParseException {
        String[] sqls = new String[] { "select \n a \n + \n b \n from t", //
                "select\na\n+\nb\nfrom t", //
                "select \r\n a \r\n + \r\n b \r\n from t", //
                "select\r\na\r\n+\r\nb\r\nfrom t" };

        for (String sql : sqls) {
            SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getSelectList().get(0);
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
            String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
            Preconditions.checkArgument(substring.startsWith("a"));
            Preconditions.checkArgument(substring.endsWith("b"));
        }

    }


    @Test
    public void testPosWithBrackets() throws SqlParseException {
        String[] sqls = new String[] {
                "select (   a + b) * (c+ d     ) from t", "select (a+b) * (c+d) from t",
                "select (a + b) * (c+ d) from t", "select (a+b) * (c+d) from t",
        };

        for (String sql : sqls) {
            SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getSelectList().get(0);
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
            String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
            Preconditions.checkArgument(substring.startsWith("("));
            Preconditions.checkArgument(substring.endsWith(")"));
        }
    }
}
