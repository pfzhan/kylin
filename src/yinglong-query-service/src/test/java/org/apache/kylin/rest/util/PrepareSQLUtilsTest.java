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

package org.apache.kylin.rest.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import io.kyligence.kap.query.engine.PrepareSqlStateParam;
import org.junit.Assert;
import org.junit.Test;

public class PrepareSQLUtilsTest {

    void verifyPrepareResult(String prepareSQL, String[] paramValues, String expectedResult) {
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[paramValues.length];
        for (int i = 0; i < paramValues.length; i++) {
            params[i] = new PrepareSqlStateParam(String.class.getCanonicalName(), paramValues[i]);
        }
        verifyPrepareResult(prepareSQL, params, expectedResult);
    }

    void verifyPrepareResult(String prepareSQL, PrepareSqlStateParam[] params, String expectedResult) {
        Assert.assertEquals(expectedResult, PrepareSQLUtils.fillInParams(prepareSQL, params));
    }

    @Test
    public void testPrepareSQL() {
        verifyPrepareResult("select a from b where c = ? and d = ?", new String[] { "123", "d'2019-01-01'" },
                "select a from b where c = '123' and d = 'd'2019-01-01''");
        verifyPrepareResult("select \"a\" from \"b\" where \"c\" = ? and \"e\" = 'abc' and d = ?;",
                new String[] { "123", "d'2019-01-01'" },
                "select \"a\" from \"b\" where \"c\" = '123' and \"e\" = 'abc' and d = 'd'2019-01-01'';");
        verifyPrepareResult(
                "select * from (select \"a\", '?' as q from \"b\" where \"c\" = ? and \"e\" = 'abc' and d = ?) join (select \"b\" from z where x = ?)",
                new String[] { "123", "d'2019-01-01'", "abcdef" },
                "select * from (select \"a\", '?' as q from \"b\" where \"c\" = '123' and \"e\" = 'abc' and d = 'd'2019-01-01'') join (select \"b\" from z where x = 'abcdef')");
        verifyPrepareResult("select a from b where c = ? and d = ? and e = ? and f = ? and g = ? and h = ? and i = ? and j = ? and k = ?",
                new PrepareSqlStateParam[] {
                        new PrepareSqlStateParam(Integer.class.getCanonicalName(), "123"),
                        new PrepareSqlStateParam(Double.class.getCanonicalName(), "123.0"),
                        new PrepareSqlStateParam(String.class.getCanonicalName(), "a string"),
                        new PrepareSqlStateParam(Date.class.getCanonicalName(), "2019-01-01"),
                        new PrepareSqlStateParam(Timestamp.class.getCanonicalName(),
                                "2019-01-01 00:12:34.123"),
                        new PrepareSqlStateParam(Short.class.getCanonicalName(), "-128"),
                        new PrepareSqlStateParam(Long.class.getCanonicalName(), "-2147483648"),
                        new PrepareSqlStateParam(Boolean.class.getCanonicalName(), "true"),
                        new PrepareSqlStateParam(BigDecimal.class.getCanonicalName(), "-9223372036854775"),
                },
                "select a from b where c = 123 and d = 123.0 and e = 'a string' and f = date'2019-01-01' and g = timestamp'2019-01-01 00:12:34.123' "
                        + "and h = -128 and i = -2147483648 and j = true and k = -9223372036854775");
    }

}
