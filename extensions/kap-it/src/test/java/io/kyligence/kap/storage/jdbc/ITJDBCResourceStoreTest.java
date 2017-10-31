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

package io.kyligence.kap.storage.jdbc;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.log4j.component.helpers.MessageFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.persistence.JDBCConnectionManager;
import io.kyligence.kap.common.persistence.JDBCResourceStore;

@Ignore
public class ITJDBCResourceStoreTest extends HBaseMetadataTestCase {

    private static final String LARGE_CELL_PATH = "/cube/_test_large_cell.json";
    private static final String Large_Content = "THIS_IS_A_LARGE_CELL";
    private KylinConfig kylinConfig;
    private JDBCConnectionManager connectionManager;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        connectionManager = JDBCConnectionManager.getConnectionManager();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConnectJDBC() throws Exception {
        Connection conn = null;
        try {
            conn = connectionManager.getConn();
            assertNotNull(conn);
        } finally {
            JDBCConnectionManager.closeQuietly(conn);
        }
    }

    @Test
    public void testJdbcBasicFunction() throws Exception {
        Connection conn = null;
        Statement statement = null;
        String createTableSql = "CREATE TABLE test(col1 VARCHAR (10), col2 INTEGER )";
        String dropTableSql = "DROP TABLE IF EXISTS test";
        try {
            conn = connectionManager.getConn();
            statement = conn.createStatement();
            statement.executeUpdate(dropTableSql);
            statement.executeUpdate(createTableSql);
            statement.executeUpdate(dropTableSql);
        } finally {
            JDBCConnectionManager.closeQuietly(statement);
            JDBCConnectionManager.closeQuietly(conn);
        }
    }


//   Support other db except mysql
//   @Test
//    public void testGetDbcpProperties() {
//        Properties prop = JDBCConnectionManager.getConnectionManager().getDbcpProperties();
//        assertEquals("com.mysql.jdbc.Driver", prop.get("driverClassName"));
//    }

    @Test
    public void testMsgFormatter() {
        System.out.println(MessageFormatter.format("{}:{}", "a", "b"));
    }

    @Test
    public void testResourceStoreBasic() throws Exception {
        ResourceStoreTest.testAStore(ResourceStoreTest.mockUrl("jdbc", kylinConfig), kylinConfig);
    }

    @Test
    public void testJDBCStoreWithLargeCell() throws Exception {
        JDBCResourceStore store = null;
        StringEntity content = new StringEntity(Large_Content);
        String largePath = "/large/large.json";
        try {
            String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                    ResourceStoreTest.mockUrl("jdbc", kylinConfig));
            store = new JDBCResourceStore(kylinConfig, kylinConfig.getMetadataUrl());
            store.deleteResource(largePath);
            store.putResource(largePath, content, StringEntity.serializer);
            assertTrue(store.exists(largePath));
            StringEntity t = store.getResource(largePath, StringEntity.class, StringEntity.serializer);
            assertEquals(content, t);
            store.deleteResource(LARGE_CELL_PATH);
            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            if (store != null)
                store.deleteResource(LARGE_CELL_PATH);
        }
    }

    @Ignore
    @Test
    public void testPerformance() throws Exception {
        ResourceStoreTest.testPerformance(ResourceStoreTest.mockUrl("jdbc", kylinConfig), kylinConfig);
        ResourceStoreTest.testPerformance(ResourceStoreTest.mockUrl("hbase", kylinConfig), kylinConfig);
    }

    @Test
    public void testMaxCell() throws Exception {
        byte[] data = new byte[500 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) 0;
        }
        JDBCResourceStore store = null;
        ByteEntity content = new ByteEntity(data);
        try {
            String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                    ResourceStoreTest.mockUrl("jdbc", kylinConfig));
            store = new JDBCResourceStore(kylinConfig, kylinConfig.getMetadataUrl());
            store.deleteResource(LARGE_CELL_PATH);
            store.putResource(LARGE_CELL_PATH, content, ByteEntity.serializer);
            assertTrue(store.exists(LARGE_CELL_PATH));
            ByteEntity t = store.getResource(LARGE_CELL_PATH, ByteEntity.class, ByteEntity.serializer);
            assertEquals(content, t);
            store.deleteResource(LARGE_CELL_PATH);
            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            if (store != null)
                store.deleteResource(LARGE_CELL_PATH);
        }
    }

    @SuppressWarnings("serial")
    public static class ByteEntity extends RootPersistentEntity {

        public static final Serializer<ByteEntity> serializer = new Serializer<ByteEntity>() {

            @Override
            public void serialize(ByteEntity obj, DataOutputStream out) throws IOException {
                byte[] data = obj.getData();
                out.writeInt(data.length);
                out.write(data);
            }

            @Override
            public ByteEntity deserialize(DataInputStream in) throws IOException {
                int length = in.readInt();
                byte[] bytes = new byte[length];
                in.read(bytes);
                return new ByteEntity(bytes);
            }
        };
        byte[] data;

        public ByteEntity() {

        }

        public ByteEntity(byte[] data) {
            this.data = data;
        }

        public static Serializer<ByteEntity> getSerializer() {
            return serializer;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }
    }
}
