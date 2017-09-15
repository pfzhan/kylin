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

package io.kyligence.kap.rest.security;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.security.QuerACLTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.ColumnACLManager;

public class QueryWithColumnACLTest extends LocalFileMetadataTestCase {
    private static final String PROJECT = "DEFAULT";
    private static final String ADMIN = "ADMIN";
    private static final String MODELER = "MODELER";
    private static final String STREAMING_TABLE = "DEFAULT.STREAMING_TABLE";
    private static final String TEST_KYLIN_FACT_TABLE = "DEFAULT.TEST_KYLIN_FACT";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata();
        KylinConfig.getInstanceFromEnv().setProperty(
                "kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
    }

    @Test
    public void testNormalQuery() throws SQLException {
        QuerACLTestUtil.setUser(ADMIN);
        QuerACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");
    }

    @Test
    public void testFailQuery() throws SQLException, IOException {
        QuerACLTestUtil.setUser(MODELER);
        QuerACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");

        // add column acl, query fail
        QuerACLTestUtil.setUser(ADMIN);
        getColumnACLManager().addColumnACL(PROJECT, ADMIN, STREAMING_TABLE, Sets.newHashSet("MINUTE_START"));
        try {
            QuerACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (SQLException e) {
            Assert.assertEquals("Query failed.Access column:DEFAULT.STREAMING_TABLE.MINUTE_START denied", e.getCause().getMessage());
        }

        // query another column, success
        QuerACLTestUtil.mockQuery(PROJECT, "select HOUR_START from STREAMING_TABLE");

        // remove acl, query success
        getColumnACLManager().deleteColumnACL(PROJECT, ADMIN, STREAMING_TABLE);
        QuerACLTestUtil.mockQuery(PROJECT, "select * from STREAMING_TABLE");
    }

    @Ignore
    @Test
    public void testColumnACLWithCC() throws IOException, SQLException {
        // ccName: BUYER_ID_AND_COUNTRY_NAME
        // ccExp: CONCAT(BUYER_ACCOUNT.ACCOUNT_ID, BUYER_COUNTRY.NAME)

        QuerACLTestUtil.setUser(ADMIN);
        getColumnACLManager().addColumnACL(PROJECT, ADMIN, TEST_KYLIN_FACT_TABLE, Sets.newHashSet("NAME"));
        try {
            QuerACLTestUtil.mockQuery(PROJECT, "select SELLER_ID_AND_COUNTRY_NAME from TEST_KYLIN_FACT");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (SQLException e) {
            Assert.assertEquals("Query failed.Access column:DEFAULT.TEST_KYLIN_FACT.NAME denied",
                    e.getCause().getMessage());
        }

        // query another column, success
        QuerACLTestUtil.mockQuery(PROJECT, "select ACCOUNT_ID from TEST_KYLIN_FACT");

        // remove acl, query success
        getColumnACLManager().deleteColumnACL(PROJECT, ADMIN, STREAMING_TABLE);
        QuerACLTestUtil.mockQuery(PROJECT, "select SELLER_ID_AND_COUNTRY_NAME from TEST_KYLIN_FACT");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private ColumnACLManager getColumnACLManager() {
        return ColumnACLManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}
