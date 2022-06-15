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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.metadata.project.NProjectManager;

@RunWith(MockitoJUnitRunner.class)
public class RawSqlTest {

    private static final String SQL = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2 -- comment1;\n"
            + "from table -- comment2\n" + "/* comment3\n" + "   comment4;\n" + "   comment5\n" + "*/\n"
            + "limit /* comment6 */ 10; -- comment7;";

    private static final String STATEMENT_STRING = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2\n"
            + "from table\n" + "limit 10";

    private static final String FULL_TEXT_STRING = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2 -- comment1;\n"
            + "from table -- comment2\n" + "/* comment3\n" + "   comment4;\n" + "   comment5\n" + "*/\n"
            + "limit /* comment6 */ 10 -- comment7;";

    private static final String SQL2 = "select * from table1";

    private RawSql rawSql;
    private RawSql rawSql2;

    @Before
    public void setUp() throws Exception {
        rawSql = new RawSqlParser(SQL).parse();
        rawSql2 = new RawSqlParser(SQL2).parse();
    }

    @Test
    public void testGetStatementString() {
        assertNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
        assertEquals(STATEMENT_STRING, rawSql.getStatementString());
        assertNotNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
    }

    @Test
    public void testGetFullTextString() {
        assertNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
        assertEquals(FULL_TEXT_STRING, rawSql.getFullTextString());
        assertNotNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
    }

    @Test
    public void testAutoAppendLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(-1);
            rawSql2.autoAppendLimit(kylinConfig, 20);
            assertEquals(SQL2 + "\nLIMIT 20", rawSql2.getStatementString());

            rawSql2.autoAppendLimit(kylinConfig, 20, 10);
            assertEquals(SQL2 + "\nLIMIT 20\nOFFSET 10", rawSql2.getStatementString());
        }
    }

    @Test
    public void testAutoAppendLimitForceLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(15);
            rawSql2.autoAppendLimit(kylinConfig, 0, 0);
            assertEquals(SQL2 + "\nLIMIT 15", rawSql2.getStatementString());
        }
    }

    @Test
    public void testClearCache() {
        assertNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
        rawSql.getStatementString();
        assertNotNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));

        assertNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
        rawSql.getFullTextString();
        assertNotNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));

        ReflectionTestUtils.invokeMethod(rawSql, "clearCache");
        assertNull(ReflectionTestUtils.getField(rawSql, "statementStringCache"));
        assertNull(ReflectionTestUtils.getField(rawSql, "fullTextStringCache"));
    }

    @Test
    public void testIsSelectStatement() throws Exception {
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(rawSql, "isSelectStatement"));

        RawSql withRawSql = new RawSqlParser("( with table_tmp as (select col1 from table1))").parse();
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(withRawSql, "isSelectStatement"));

        RawSql explainRawSql = new RawSqlParser("explain select col1 from table1").parse();
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(explainRawSql, "isSelectStatement"));

        RawSql createRawSql = new RawSqlParser("create table xxx as (select col1, col2 from table1)").parse();
        assertEquals(Boolean.FALSE, ReflectionTestUtils.invokeMethod(createRawSql, "isSelectStatement"));
    }

    @Test
    public void testSqlLimitAndForceLimit() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1 limit 10").parse();
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(15);
            tmpSql.autoAppendLimit(kylinConfig, 0);
            assertEquals("select * from table1 limit 10", tmpSql.getStatementString());
        }
    }

    @Test
    public void testProjectForceLimitEnabled() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1").parse();
        try (MockedStatic<NProjectManager> nProjectManagerMockedStatic = Mockito.mockStatic(NProjectManager.class)) {
            NProjectManager projectManager = Mockito.mock(NProjectManager.class);
            nProjectManagerMockedStatic.when(() -> NProjectManager.getInstance(Mockito.any()))
                    .thenReturn(projectManager);
            KylinConfigExt kylinConfigExt = Mockito.mock(KylinConfigExt.class);
            when(kylinConfigExt.getForceLimit()).thenReturn(14);
            tmpSql.autoAppendLimit(kylinConfigExt, 0);
            assertEquals("select * from table1" + "\n" + "LIMIT 14", tmpSql.getStatementString());
        }
    }

    @Test
    public void testMaxResultRowsEnabled() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1").parse();
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            tmpSql.autoAppendLimit(kylinConfig, 16);
            assertEquals("select * from table1" + "\n" + "LIMIT 15", tmpSql.getStatementString());
        }
    }

    @Test
    public void testCompareMaxResultRowsAndLimit() throws ParseException {
        RawSql tmpSql = new RawSqlParser("select * from table1").parse();
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            tmpSql.autoAppendLimit(kylinConfig, 13);
            assertEquals("select * from table1" + "\n" + "LIMIT 13", tmpSql.getStatementString());
        }
    }
}
