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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class RawSqlTest {

    private static final String SQL = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2 -- comment1;\n"
            + "from table -- comment2\n"
            + "/* comment3\n"
            + "   comment4;\n"
            + "   comment5\n"
            + "*/\n"
            + "limit /* comment6 */ 10; -- comment7;";

    private static final String STATEMENT_STRING = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2\n"
            + "from table\n"
            + "limit 10";

    private static final String FULL_TEXT_STRING = "select /*+ MODEL_PRIORITY(model1, model2) */ 'col1-;1', col2 -- comment1;\n"
            + "from table -- comment2\n"
            + "/* comment3\n"
            + "   comment4;\n"
            + "   comment5\n"
            + "*/\n"
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
            rawSql2.autoAppendLimit(20);
            assertEquals(SQL2 + "\nLIMIT 20", rawSql2.getStatementString());

            rawSql2.autoAppendLimit(20, 10);
            assertEquals(SQL2 + "\nLIMIT 20\nOFFSET 10", rawSql2.getStatementString());
        }
    }

    @Test
    public void testAutoAppendLimitForceLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            when(kylinConfig.getForceLimit()).thenReturn(15);
            rawSql2.autoAppendLimit(0, 0);
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
}
