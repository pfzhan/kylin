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

package io.kyligence.kap.query.engine;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;

import io.kyligence.kap.query.engine.data.TableSchema;

public class SchemaMetaData {

    private ProjectSchemaFactory projectSchemaFactory;

    public SchemaMetaData(String project, KylinConfig kylinConfig) {
        projectSchemaFactory = new ProjectSchemaFactory(project, kylinConfig);
    }

    public List<TableSchema> getTables() {
        return projectSchemaFactory.createProjectRootSchema().getSubSchemaMap().values().stream()
                .flatMap(schema -> schema.getTableNames().stream().map(tableName -> {
                    Table table = schema.getTable(tableName, false).getTable();
                    return convertToTableSchema(
                            Constant.FakeCatalogName,
                            schema.getName() == null ? Constant.FakeSchemaName : schema.getName(),
                            tableName,
                            table);
                })).collect(Collectors.toList());
    }

    private TableSchema convertToTableSchema(String catalogName, String schemaName, String tableName, Table table) {
        return new TableSchema(catalogName, schemaName, tableName, table.getJdbcTableType().toString(), null,
                RelColumnMetaDataExtractor.getColumnMetadata(table.getRowType(javaTypeFactory())));
    }

    private JavaTypeFactory javaTypeFactory() {
        return new TypeSystem().javaTypeFactory();
    }

}
