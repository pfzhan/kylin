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
package io.kyligence.kap.source.jdbc;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcTestBase extends NLocalFileMetadataTestCase {
    protected static JdbcConnector connector = null;
    protected static Connection h2Conn = null;
    protected static H2Database h2Db = null;

    @BeforeClass
    public static void setUp() throws SQLException {
        staticCreateTestMetadata();
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "testing");

        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        h2Conn = connector.getConnection();

        h2Db = new H2Database(h2Conn, getTestConfig(), "default");
        h2Db.loadAllTables();
    }

    @AfterClass
    public static void after() throws SQLException {
        h2Db.dropAll();
        DBUtils.closeQuietly(h2Conn);
        staticCleanupTestMetadata();
    }
}
