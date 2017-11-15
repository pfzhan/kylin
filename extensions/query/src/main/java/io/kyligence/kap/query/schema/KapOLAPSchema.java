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

import static org.apache.kylin.query.schema.OLAPSchemaFactory.exposeMore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.query.schema.OLAPSchema;

public class KapOLAPSchema extends OLAPSchema {
    private KylinConfig config;
    private String projectName;
    private String schemaName;
    private boolean exposeMore;

    public KapOLAPSchema(String project, String schemaName, boolean exposeMore) {
        super(project, schemaName, exposeMore);
        this.projectName = project;
        this.schemaName = schemaName;
        this.exposeMore = exposeMore;
        init();
    }

    private void init() {
        this.config = KylinConfig.getInstanceFromEnv();
    }

    @Override
    public Map<String, Table> getTableMap() {
        return buildTableMap();
    }

    private Map<String, Table> buildTableMap() {
        Map<String, Table> olapTables = new HashMap<String, Table>();

        Collection<TableDesc> projectTables = ProjectManager.getInstance(config).listExposedTables(projectName,
                exposeMore());

        for (TableDesc tableDesc : projectTables) {
            if (tableDesc.getDatabase().equals(schemaName)) {
                final String tableName = tableDesc.getName();//safe to use tableDesc.getName() here, it is in a DB context now
                final KapOLAPTable table = new KapOLAPTable(this, tableDesc, exposeMore);
                olapTables.put(tableName, table);
                //logger.debug("Project " + projectName + " exposes table " + tableName);
            }
        }

        return olapTables;
    }
}
