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
package org.apache.kylin.sdk.datasource.adaptor;

import java.sql.SQLException;

import org.apache.kylin.sdk.datasource.framework.JdbcConnectorTest;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class AdaptorsTest extends JdbcConnectorTest {

    @Test
    public void testMySQLAdaptor() throws Exception {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.MysqlAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "mysql");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertFalse(connector.listDatabases().contains("EDW"));
        Assert.assertTrue(connector.listTables("DEFAULT").isEmpty());
        Assert.assertFalse(connector.listTables("DB").isEmpty());
        Assert.assertNotNull(connector.getTable("DB", "ROLES"));
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testMssqlAdaptor() throws SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.MssqlAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "mssql");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        AbstractJdbcAdaptor adaptor = connector.getAdaptor();
        Assert.assertEquals("select 1", adaptor.fixSql("select 1"));
        Assert.assertEquals("float", adaptor.toSourceTypeName("DOUBLE"));
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertNotNull(connector.getTable("DB", "ROLES"));
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testSnowflakeAdaptor() throws SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.SnowflakeAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "snowflake");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        AbstractJdbcAdaptor adaptor = connector.getAdaptor();
        Assert.assertEquals("select 1", adaptor.fixSql("select 1"));
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertFalse(connector.listTables("DEFAULT").isEmpty());
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }

    @Test
    public void testSQLDWAdaptor() throws SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.adaptor", "org.apache.kylin.sdk.datasource.adaptor.SQLDWAdaptor");
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "sqldw");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertNotNull(connector.listColumns("DB", "ROLES"));
    }


}
