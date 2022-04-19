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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.QueryExtension;
import io.kyligence.kap.query.engine.view.ViewAnalyzer;
import io.kyligence.kap.query.engine.view.ViewSchema;
import io.kyligence.kap.query.schema.KapOLAPSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * factory that create and construct schemas within a project
 */
@Slf4j
class ProjectSchemaFactory {

    private final String projectName;
    private final KylinConfig kylinConfig;
    private Map<String, List<TableDesc>> schemasMap;
    private Map<String, List<NDataModel>> modelsMap;
    private String defaultSchemaName;

    ProjectSchemaFactory(String projectName, KylinConfig kylinConfig) {
        this.projectName = projectName;
        this.kylinConfig = kylinConfig;

        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        String user = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
        Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
        schemasMap = QueryExtension.getFactory().getSchemaMapExtension().getAuthorizedTablesAndColumns(kylinConfig,
                projectName, aclDisabledOrIsAdmin(aclInfo), user, groups);
        removeStreamingTables(schemasMap);
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

    /**
     * remove streaming tables when streaming function is disabled
     */
    private void removeStreamingTables(Map<String, List<TableDesc>> schemasMap) {
        schemasMap.values().stream().forEach(tableDescList -> tableDescList
                .removeIf(tableDesc -> !NTableMetadataManager.isTableAccessible(tableDesc)));
        schemasMap.keySet().removeIf(key -> CollectionUtils.isEmpty(schemasMap.get(key)));
    }

    private boolean aclDisabledOrIsAdmin(QueryContext.AclInfo aclInfo) {
        return !kylinConfig.isAclTCREnabled()
                || Objects.nonNull(aclInfo) && (CollectionUtils.isNotEmpty(aclInfo.getGroups())
                        && aclInfo.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals))
                || Objects.nonNull(aclInfo) && aclInfo.isHasAdminPermission();
    }

    public CalciteSchema createProjectRootSchema() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        addProjectSchemas(rootSchema);

        return rootSchema;
    }

    public void setDefaultSchemaName(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
    }

    public String getDefaultSchema() {
        return defaultSchemaName;
    }

    private void addProjectSchemas(CalciteSchema parentSchema) {

        for (String schemaName : schemasMap.keySet()) {
            CalciteSchema added = parentSchema.add(schemaName, createSchema(schemaName));
            addUDFs(added);
        }
    }

    public void addModelViewSchemas(CalciteSchema parentSchema, ViewAnalyzer viewAnalyzer) {
        final String viewSchemaName = projectName;

        ViewSchema viewSchema = new ViewSchema(viewSchemaName, viewAnalyzer);
        CalciteSchema schema = getOrCreateViewSchema(parentSchema, viewSchema);
        SchemaPlus plus = schema.plus();

        List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, projectName).listOnlineDataModels();
        for (NDataModel model : models) {
            if (schema.getTable(model.getAlias(), false) == null) {
                viewSchema.addModel(plus, model);
            } else {
                log.warn("Auto model view creation for {}.{} failed, name collision with source tables", viewSchemaName,
                        viewSchemaName);
            }
        }
    }

    private CalciteSchema getOrCreateViewSchema(CalciteSchema parentSchema, ViewSchema viewSchema) {
        String schemaName = viewSchema.getSchemaName();
        CalciteSchema existingSchema = parentSchema.getSubSchema(schemaName, false);
        if (existingSchema != null) {
            return existingSchema;
        }

        CalciteSchema addedViewSchema = parentSchema.add(schemaName, viewSchema);
        addUDFs(addedViewSchema);
        return addedViewSchema;
    }

    private Schema createSchema(String schemaName) {
        return new KapOLAPSchema(projectName, schemaName, schemasMap.get(schemaName), modelsMap);
    }

    private void addUDFs(CalciteSchema calciteSchema) {
        for (Map.Entry<String, Function> entry : UDFRegistry.allUdfMap.entries()) {
            calciteSchema.plus().add(entry.getKey().toUpperCase(Locale.ROOT), entry.getValue());
        }
    }
}
