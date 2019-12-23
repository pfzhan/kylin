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

package org.apache.kylin.jdbc;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.jdbc.KylinMeta.KMetaCatalog;
import org.apache.kylin.jdbc.KylinMeta.KMetaColumn;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;
import org.apache.kylin.jdbc.KylinMeta.KMetaSchema;
import org.apache.kylin.jdbc.KylinMeta.KMetaTable;
import org.apache.kylin.jdbc.json.SQLResponseStub;
import org.apache.kylin.jdbc.json.StatementParameter;

/**
 */
public class DummyClient extends KylinClient {

    public DummyClient(KylinConnection conn) {
        super(conn);
    }

    @Override
    public void connect() throws IOException {
    }

    @Override
    public KMetaProject retrieveMetaData(String project) throws IOException {
        List<KMetaColumn> columns = new ArrayList<KMetaColumn>();

        KMetaTable table = new KMetaTable("dummy", "dummy", "dummy", "dummy", columns);
        List<KMetaTable> tables = new ArrayList<KMetaTable>();
        tables.add(table);

        KMetaSchema schema = new KMetaSchema("dummy", "dummy", tables);
        List<KMetaSchema> schemas = new ArrayList<KMetaSchema>();
        schemas.add(schema);

        KMetaCatalog catalog = new KMetaCatalog("dummay", schemas);
        List<KMetaCatalog> catalogs = new ArrayList<KMetaCatalog>();
        catalogs.add(catalog);

        return new KMetaProject(project, catalogs);
    }

    @Override
    public SQLResponseStub executeKylinQuery(String sql, List<StatementParameter> params,
                                             Map<String, String> queryToggles) throws IOException {
        SQLResponseStub sqlResponseStub = new SQLResponseStub();

        List<SQLResponseStub.ColumnMetaStub> meta = new ArrayList<>();
        SQLResponseStub.ColumnMetaStub column1 = new SQLResponseStub.ColumnMetaStub();
        column1.setColumnType(Types.VARCHAR);
        column1.setColumnTypeName("varchar");
        column1.setIsNullable(1);
        meta.add(column1);

        SQLResponseStub.ColumnMetaStub column2 = new SQLResponseStub.ColumnMetaStub();
        column2.setColumnType(Types.VARCHAR);
        column2.setColumnTypeName("varchar");
        column2.setIsNullable(1);
        meta.add(column2);

        SQLResponseStub.ColumnMetaStub column3 = new SQLResponseStub.ColumnMetaStub();
        column3.setColumnType(Types.VARCHAR);
        column3.setColumnTypeName("varchar");
        column3.setIsNullable(1);
        meta.add(column3);

        SQLResponseStub.ColumnMetaStub column4 = new SQLResponseStub.ColumnMetaStub();
        column4.setColumnType(Types.DATE);
        column4.setColumnTypeName("date");
        column4.setIsNullable(1);
        meta.add(column4);

        SQLResponseStub.ColumnMetaStub column5 = new SQLResponseStub.ColumnMetaStub();
        column5.setColumnType(Types.TIME);
        column5.setColumnTypeName("time");
        column5.setIsNullable(1);
        meta.add(column5);

        SQLResponseStub.ColumnMetaStub column6 = new SQLResponseStub.ColumnMetaStub();
        column6.setColumnType(Types.TIMESTAMP);
        column6.setColumnTypeName("timestamp");
        column6.setIsNullable(1);
        meta.add(column6);

        sqlResponseStub.setColumnMetas(meta);

        List<String[]> data = new ArrayList<String[]>();

        String[] row = new String[] { "foo", "bar", "tool", "2019-04-27", "17:30:03.03", "2019-04-27 17:30:03.03" };
        data.add(row);

        sqlResponseStub.setResults(data);

        return sqlResponseStub;
    }

    @Override
    public String getTimeZoneFromKylin() {
        return "America/New_York";
    }

    @Override
    public void close() throws IOException {
    }

}
