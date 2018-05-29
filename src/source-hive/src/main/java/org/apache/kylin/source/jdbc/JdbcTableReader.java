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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.source.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.source.IReadableTable.TableReader;
import org.apache.kylin.source.hive.DBConnConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of TableReader with HCatalog for Hive table.
 */
public class JdbcTableReader implements TableReader {
    private static final Logger logger = LoggerFactory.getLogger(JdbcTableReader.class);
    
    private String dbName;
    private String tableName;

    private DBConnConf dbconf;
    private String dialect;
    private Connection jdbcCon;
    private Statement statement;
    private ResultSet rs;
    private int colCount;

    /**
     * Constructor for reading whole hive table
     * @param dbName
     * @param tableName
     * @throws IOException
     */
    public JdbcTableReader(String dbName, String tableName) throws IOException {
        this.dbName = dbName;
        this.tableName = tableName;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String connectionUrl = config.getJdbcConnectionUrl();
        String driverClass = config.getJdbcDriver();
        String jdbcUser = config.getJdbcUser();
        String jdbcPass = config.getJdbcPass();
        dbconf = new DBConnConf(driverClass, connectionUrl, jdbcUser, jdbcPass);
        this.dialect = config.getJdbcDialect();
        jdbcCon = SqlUtil.getConnection(dbconf);
        String sql = String.format("select * from %s.%s", dbName, tableName);
        try {
            statement = jdbcCon.createStatement();
            rs = statement.executeQuery(sql);
            colCount = rs.getMetaData().getColumnCount();
        }catch(SQLException e){
            throw new IOException(String.format("error while exec %s", sql), e);
        }
        
    }

    @Override
    public boolean next() throws IOException {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String[] getRow() {
        String[] ret = new String[colCount];
        for (int i=1; i<=colCount; i++){
            try {
                Object o = rs.getObject(i);
                ret[i-1] = (o == null? null:o.toString());
            }catch(Exception e){
                logger.error("", e);
            }
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        SqlUtil.closeResources(jdbcCon, statement);
    }

    public String toString() {
        return "jdbc table reader for: " + dbName + "." + tableName;
    }
}
