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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class SingleSqlNodeReplacerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testReplace() throws SqlParseException {
        ConvMaster convMaster = new ConvMaster(null, null);
        SingleSqlNodeReplacer replacer = new SingleSqlNodeReplacer(convMaster);

        SqlNode origin = SqlParser.create("1 = 1 and (a = 'a' or  a = 'aa')").parseExpression();
        SqlNode sqlNodeTryToFind = SqlParser.create("a").parseExpression();
        SqlNode sqlNodeToReplace = SqlParser.create("to_char(a,'yyyy')").parseExpression();

        replacer.setSqlNodeTryToFind(sqlNodeTryToFind);
        replacer.setSqlNodeToReplace(sqlNodeToReplace);

        SqlNode replaced = origin.accept(replacer);

        SqlPrettyWriter writer = new SqlPrettyWriter(SqlDialect.CALCITE);
        String sqlReplace = writer.format(replaced);

        Assert.assertEquals("1 = 1 AND (TO_CHAR(\"A\", 'yyyy') = 'a' OR TO_CHAR(\"A\", 'yyyy') = 'aa')", sqlReplace);

        origin = SqlParser.create("aa = 1 and (a = 'a' or  a = 'aa')").parseExpression();
        replaced = origin.accept(replacer);
        writer.reset();
        sqlReplace = writer.format(replaced);

        Assert.assertEquals("\"AA\" = 1 AND (TO_CHAR(\"A\", 'yyyy') = 'a' OR TO_CHAR(\"A\", 'yyyy') = 'aa')",
                sqlReplace);
    }

}
