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

package io.kyligence.kap.query.schema;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.schema.OLAPSchema;

import io.kyligence.kap.metadata.model.NDataModel;

public class KapOLAPSchema extends OLAPSchema {
    private String schemaName;
    private List<TableDesc> tables;
    private Map<String, List<NDataModel>> modelsMap;

    public KapOLAPSchema(String project, String schemaName, List<TableDesc> tables, Map<String, List<NDataModel>> modelsMap) {
        super(project, schemaName, tables);
        this.schemaName = schemaName;
        this.tables = tables;
        this.modelsMap = modelsMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return createTableMap();
    }

    public boolean hasTables() {
        return tables != null && !tables.isEmpty();
    }

    private Map<String, Table> createTableMap() {
        Map<String, Table> olapTables = new HashMap<>();

        for (TableDesc tableDesc : tables) {
            final String tableName = tableDesc.getName();//safe to use tableDesc.getUuid() here, it is in a DB context now
            final KapOLAPTable table = new KapOLAPTable(this, tableDesc, modelsMap);
            olapTables.put(tableName, table);
        }

        return olapTables;
    }
}
