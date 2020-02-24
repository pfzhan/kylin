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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.schema.KapOLAPSchema;

/**
 * factory that create and construct schemas within a project
 */
public class KapProjectSchemaFactory {

    private final String projectName;
    private final KylinConfig kylinConfig;
    private HashMap<String, Integer> schemaCounts;
    private String defaultSchemaName;

    public KapProjectSchemaFactory(String projectName, KylinConfig kylinConfig) {
        this.projectName = projectName;
        this.kylinConfig = kylinConfig;


        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        Collection<TableDesc> tables = npr.listExposedTables(projectName, kylinConfig.isPushDownEnabled());

        // "database" in TableDesc correspond to our schema
        // the logic to decide which schema to be "default" in calcite:
        // if some schema are named "default", use it.
        // other wise use the schema with most tables
        schemaCounts = DatabaseDesc.extractDatabaseOccurenceCounts(tables);
        String majoritySchemaName = npr.getDefaultDatabase(projectName);
        // UT
        if (Objects.isNull(majoritySchemaName)) {
            majoritySchemaName = getDatabaseByMaxTables(schemaCounts);
        }
        defaultSchemaName = majoritySchemaName;
    }

    public CalciteSchema createProjectRootSchema() {
        CalciteSchema calciteSchema = CalciteSchema.createRootSchema(true);
        addProjectSchemas(calciteSchema);
        return calciteSchema;
    }

    public String getDefaultSchema() {
        return defaultSchemaName;
    }

    public void addProjectSchemas(CalciteSchema parentSchema) {

        for (String schemaName : schemaCounts.keySet()) {
            CalciteSchema added = parentSchema.add(schemaName, createSchema(schemaName));
            addUDFs(added);
        }
    }

    private Schema createSchema(String schemaName) {
        return new KapOLAPSchema(projectName, schemaName, kylinConfig.isPushDownEnabled());
    }

    private void addUDFs(CalciteSchema calciteSchema) {
        for (UDFRegistry.UDFDefinition udfDef : UDFRegistry.getInstance(kylinConfig, projectName).getUdfDefinitions()) {
            // TODO move add functions here if necessary
            ModelHandler.addFunctions(calciteSchema.plus(), udfDef.getName(), udfDef.getPaths(), udfDef.getClassName(), udfDef.getMethodName(), false);
        }
    }

    private static String getDatabaseByMaxTables(Map<String, Integer> schemaCounts) {
        String majoritySchemaName = ProjectInstance.DEFAULT_DATABASE;
        int majoritySchemaCount = 0;
        for (Map.Entry<String, Integer> e : schemaCounts.entrySet()) {
            if (e.getKey().equalsIgnoreCase(ProjectInstance.DEFAULT_DATABASE)) {
                majoritySchemaName = e.getKey();
                break;
            }

            if (e.getValue() >= majoritySchemaCount) {
                majoritySchemaCount = e.getValue();
                majoritySchemaName = e.getKey();
            }
        }

        return majoritySchemaName;
    }

}
