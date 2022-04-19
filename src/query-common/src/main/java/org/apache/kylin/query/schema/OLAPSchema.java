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

package org.apache.kylin.query.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

/**
 * all schema info is in KapOLAPSchema, not used anymore
 * @deprecated
 */
@Deprecated
public class OLAPSchema extends AbstractSchema {

    //    private static final Logger logger = LoggerFactory.getLogger(OLAPSchema.class);

    private KylinConfig config;
    private String projectName;
    private String schemaName;
    private List<TableDesc> tables;
    private Map<String, List<NDataModel>> modelsMap;
    private String starSchemaUrl;
    private String starSchemaUser;
    private String starSchemaPassword;

    private void init() {
        this.config = KylinConfig.getInstanceFromEnv();
        this.starSchemaUrl = config.getHiveUrl();
        this.starSchemaUser = config.getHiveUser();
        this.starSchemaPassword = config.getHivePassword();
    }

    public OLAPSchema(String project, String schemaName, List<TableDesc> tables) {
        this.projectName = project;
        this.schemaName = schemaName;
        this.tables = tables;
        init();
    }

    /**
     * It is intended to skip caching, because underlying project/tables might change.
     *
     * @return
     */
    @Override
    public Map<String, Table> getTableMap() {
        return buildTableMap();
    }

    private Map<String, Table> buildTableMap() {
        Map<String, Table> olapTables = new HashMap<>();

        for (TableDesc tableDesc : tables) {
            if (tableDesc.getDatabase().equals(schemaName)) {
                final String tableName = tableDesc.getName();//safe to use tableDesc.getUuid() here, it is in a DB context now
                final OLAPTable table = new OLAPTable(this, tableDesc, modelsMap);
                olapTables.put(tableName, table);
                //logger.debug("Project " + projectName + " exposes table " + tableName);
            }
        }

        return olapTables;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean hasStarSchemaUrl() {
        return starSchemaUrl != null && !starSchemaUrl.isEmpty();
    }

    public String getStarSchemaUrl() {
        return starSchemaUrl;
    }

    public String getStarSchemaUser() {
        return starSchemaUser;
    }

    public String getStarSchemaPassword() {
        return starSchemaPassword;
    }

    public NDataModelManager getMetadataManager() {
        return NDataModelManager.getInstance(config, projectName);
    }

    public KylinConfig getConfig() {
        return config;
    }

    @VisibleForTesting
    public void setConfigOnlyInTest(KylinConfig config) {
        this.config = config;
    }

    public String getProjectName() {
        return this.projectName;
    }

}
