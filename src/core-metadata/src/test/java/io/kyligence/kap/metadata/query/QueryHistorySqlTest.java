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

package io.kyligence.kap.metadata.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class QueryHistorySqlTest {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @Test
    public void testGetSqlWithParameterBindingComment() {
        final String SQL = "select col1, col2, col3 from table1 where col1 = ? and col2 = ?";
        QueryHistorySql queryHistorySql = new QueryHistorySql(SQL, null, null);

        assertEquals(SQL, queryHistorySql.getSqlWithParameterBindingComment());

        List<QueryHistorySqlParam> params = new ArrayList<>();
        params.add(new QueryHistorySqlParam(1, "java.lang.Integer", "INTEGER", "1001"));
        params.add(new QueryHistorySqlParam(2, "java.lang.String", "VARCHAR", "Male"));
        queryHistorySql.setParams(params);

        final String SQL_WITH_PARAMS_COMMENT = SQL
                + LINE_SEPARATOR
                + LINE_SEPARATOR
                + "-- [PARAMETER BINDING]"
                + LINE_SEPARATOR
                + "-- Binding parameter [1] as [INTEGER] - [1001]"
                + LINE_SEPARATOR
                + "-- Binding parameter [2] as [VARCHAR] - [Male]"
                + LINE_SEPARATOR
                + "-- [PARAMETER BINDING END]";
        assertEquals(SQL_WITH_PARAMS_COMMENT, queryHistorySql.getSqlWithParameterBindingComment());
    }
}
