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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

public class SqlParamsFinderTest {

    @Test
    public void testParamFinder() throws SqlParseException {
        SqlParser sqlParser1 = SqlParser.create("POWER($0, $1) + AVG(LN($3)) + EXP($5)");
        SqlNode sqlPattern = sqlParser1.parseExpression();
        SqlParser sqlParser2 = SqlParser.create("POWER(3, POWER(2, POWER(2, 3))) + AVG(LN(EXP(4))) + EXP(CAST('2018-03-22' AS DATE))");
        SqlNode sqlCall = sqlParser2.parseExpression();

        SqlParamsFinder sqlParamsFinder = new SqlParamsFinder((SqlCall)sqlPattern, (SqlCall)sqlCall);
        Map<Integer, SqlNode> paramNodes =  sqlParamsFinder.getParamNodes();

        Assert.assertEquals("3", paramNodes.get(0).toString());
        Assert.assertEquals("POWER(2, POWER(2, 3))", paramNodes.get(1).toString());
        Assert.assertEquals("EXP(4)", paramNodes.get(3).toString());
        Assert.assertEquals("CAST('2018-03-22' AS DATE)", paramNodes.get(5).toString());

    }

    @Test
    public void testWindowCallParams() throws SqlParseException {
        SqlParser sqlParser1 = SqlParser.create("STDDEV_POP($0) OVER($1)");
        SqlNode sqlPattern = sqlParser1.parseExpression();
        SqlParser sqlParser2 = SqlParser.create("STDDEV_POP(C1) OVER (ORDER BY C1)");
        SqlNode sqlCall = sqlParser2.parseExpression();
        SqlParamsFinder sqlParamsFinder = SqlParamsFinder.newInstance((SqlCall)sqlPattern, (SqlCall)sqlCall, true);
        Map<Integer, SqlNode> paramNodes =  sqlParamsFinder.getParamNodes();

        Assert.assertEquals("C1", paramNodes.get(0).toString());
        Assert.assertEquals("(ORDER BY `C1`)", paramNodes.get(1).toString());
        Assert.assertTrue(paramNodes.get(1) instanceof SqlWindow);
    }
}
