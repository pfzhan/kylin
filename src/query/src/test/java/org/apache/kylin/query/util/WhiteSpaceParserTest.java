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

package org.apache.kylin.query.util;

import org.junit.Assert;
import org.junit.Test;

public class WhiteSpaceParserTest {

    private final EscapeDialect dialect = EscapeDialect.DEFAULT;

    String expected = "select ACCOUNT_ID, ACCOUNT_ID + 1, 'a     +    b', 'c* d', ACCOUNT_ID + 2, count ( * ) from KYLIN_ACCOUNT group by ACCOUNT_ID limit 400 ";

    @Test
    public void testMultiWhiteSpaces() throws ParseException {
        String sql = "select   ACCOUNT_ID, ACCOUNT_ID + 1 ,'a     +    b'  ,'c* d', ACCOUNT_ID +2,count(*) from KYLIN_ACCOUNT group  by  ACCOUNT_ID  limit  400   ";
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertEquals(expected, parsed);
    }

    @Test
    public void testNewLines() throws ParseException {
        String newLines = "select   ACCOUNT_ID, ACCOUNT_ID + 1 ,'a     +    b'  ,'c* d', ACCOUNT_ID +2,count(*) from "
                + "KYLIN_ACCOUNT "
                + "group   by  ACCOUNT_ID  "
                + "limit  400   ";
        testEqual(newLines);
    }

    @Test
    public void testCommas() throws ParseException {
        String commas = "select   ACCOUNT_ID   ,     ACCOUNT_ID + 1  ,  'a     +    b'  ,'c* d', ACCOUNT_ID +2,count(*) from "
                + "KYLIN_ACCOUNT "
                + "group   by  ACCOUNT_ID  "
                + "limit  400   ";
        testEqual(commas);
    }

    @Test
    public void testOperators() throws ParseException {
        String operators = "select   ACCOUNT_ID   ,     ACCOUNT_ID     +    1  ,  'a     +    b'  ,'c* d', ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT "
                + "group   by  ACCOUNT_ID  "
                + "limit  400   ";
        testEqual(operators);
    }

    private void testEqual(String sql) throws ParseException {
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertEquals(expected, parsed);
    }

    @Test
    public void testNormalEscapeString() throws ParseException {
        String sql = "select   ACCOUNT_ID   ,     ACCOUNT_ID     +    1  ,  'a+ b'  ,'c * d', ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT "
                + "group   by  ACCOUNT_ID  "
                + "limit  400   ";
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertNotEquals(expected, parsed);
        String real = "select ACCOUNT_ID, ACCOUNT_ID + 1, 'a+ b', 'c * d', ACCOUNT_ID + 2, count ( * ) from KYLIN_ACCOUNT group by ACCOUNT_ID limit 400 ";
        Assert.assertEquals(real, parsed);
    }

    @Test
    public void testBinaryEscapeString() throws ParseException {
        String sql = "select   ACCOUNT_ID   ,     ACCOUNT_ID     +    1  ,  x'a+ b'  ,X'c *     d', ACCOUNT_ID+2,count(*) from "
                + "KYLIN_ACCOUNT "
                + "group   by  ACCOUNT_ID  "
                + "limit  400   ";
        WhiteSpaceParser whiteSpaceParser = new WhiteSpaceParser(dialect, sql.trim());
        String parsed = whiteSpaceParser.parse();
        Assert.assertNotEquals(expected, parsed);
        String real = "select ACCOUNT_ID, ACCOUNT_ID + 1, x'a+ b', X'c *     d', ACCOUNT_ID + 2, count ( * ) from KYLIN_ACCOUNT group by ACCOUNT_ID limit 400 ";
        Assert.assertEquals(real, parsed);
    }
}
