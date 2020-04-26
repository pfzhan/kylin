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


import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.TableDesc;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.schema.KapOLAPSchema;
import org.apache.kylin.rest.constant.Constant;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * factory that create and construct schemas within a project
 */
public class ProjectSchemaFactory {

    private final String projectName;
    private final KylinConfig kylinConfig;
    private Map<String, List<TableDesc>> schemasMap;
    private Map<String, List<NDataModel>> modelsMap;
    private String defaultSchemaName;

    public ProjectSchemaFactory(String projectName, KylinConfig kylinConfig) {
        this.projectName = projectName;
        this.kylinConfig = kylinConfig;


        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        String user = QueryContext.current().getUsername();
        Set<String> groups = QueryContext.current().getGroups();
        schemasMap = AclTCRManager.getInstance(kylinConfig, projectName).getAuthorizedTablesAndColumns(user, groups, aclDisabledOrIsAdmin());
        modelsMap = NDataflowManager.getInstance(kylinConfig, projectName).getModelsGroupbyTable();

        // "database" in TableDesc correspond to our schema
        // the logic to decide which schema to be "default" in calcite:
        // if some schema are named "default", use it.
        // other wise use the schema with most tables
        String majoritySchemaName = npr.getDefaultDatabase(projectName);
        // UT
        if (Objects.isNull(majoritySchemaName)) {
            majoritySchemaName = DatabaseDesc.getDefaultDatabaseByMaxTables(schemasMap);
        }
        defaultSchemaName = majoritySchemaName;
    }

    private boolean aclDisabledOrIsAdmin() {
        val groups = QueryContext.current().getGroups();
        return !kylinConfig.isAclTCREnabled()
                || (CollectionUtils.isNotEmpty(groups) && groups.stream().anyMatch(Constant.ROLE_ADMIN::equals))
                || QueryContext.current().isHasAdminPermission();
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

        for (String schemaName : schemasMap.keySet()) {
            CalciteSchema added = parentSchema.add(schemaName, createSchema(schemaName));
            addUDFs(added);
        }
    }

    private Schema createSchema(String schemaName) {
        return new KapOLAPSchema(projectName, schemaName, schemasMap.get(schemaName), modelsMap);
    }

    private void addUDFs(CalciteSchema calciteSchema) {
        for (UDFRegistry.UDFDefinition udfDef : UDFRegistry.getInstance(kylinConfig, projectName).getUdfDefinitions()) {
            // TODO move add functions here if necessary
            ModelHandler.addFunctions(calciteSchema.plus(), udfDef.getName(), udfDef.getPaths(), udfDef.getClassName(), udfDef.getMethodName(), false);
        }
    }
}
