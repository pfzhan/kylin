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

package io.kyligence.kap.query.mockup;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MockupQueryExecutor implements Closeable {
    private final KylinConfig kylinConfig;

    private Map<String, Connection> connections = Maps.newHashMap();
    private List<File> usedFiles = Lists.newLinkedList();

    public MockupQueryExecutor(AbstractQueryRecorder queryRecorder) {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        AbstractQueryRecorder.CURRENT.set(queryRecorder);
    }

    public void execute(String projectName, String sql) throws SQLException {
        // TODO: share same code with kylin after move JDBC datasource from CacheService class
        Connection conn = connections.get(projectName);
        if (conn == null || conn.isClosed()) {
            File olapSchema = OLAPSchemaFactory.createTempOLAPJson(projectName, kylinConfig);
            usedFiles.add(olapSchema);
            conn = DriverManager.getConnection("jdbc:calcite:model=" + olapSchema.getAbsolutePath());
            connections.put(projectName, conn);
        }

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = conn.createStatement();
            sql = QueryUtil.massageSql(sql);
            resultSet = statement.executeQuery(sql);
        } finally {
            DBUtils.closeQuietly(statement);
            DBUtils.closeQuietly(resultSet);
        }
    }

    @Override
    public void close() throws IOException {
        for (Connection conn : connections.values()) {
            DBUtils.closeQuietly(conn);
        }
        for (File usedFile : usedFiles) {
            usedFile.delete();
        }
    }
}
