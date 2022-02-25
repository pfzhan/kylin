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
package io.kyligence.kap.common.persistence.metadata.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcUtilTest {

    private Connection connection;

    @Before
    public void setup() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa",
                null);
    }

    @Test
    public void testIsColumnExists() throws SQLException {
        String table = JdbcUtilTest.class.getSimpleName();
        connection.createStatement().execute("create table " + table + "(col1 int, col2 varchar)");
        Assert.assertTrue(JdbcUtil.isColumnExists(connection, table, "col1"));

        // case insensitive
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa",
                null);
        Assert.assertTrue(JdbcUtil.isColumnExists(connection, table, "cOL1"));

        // not exists
        this.connection = DriverManager.getConnection("jdbc:h2:mem:jdbc_util_test;DB_CLOSE_DELAY=-1;MODE=MySQL", "sa",
                null);
        Assert.assertFalse(JdbcUtil.isColumnExists(connection, table, "not_exists"));
    }
}