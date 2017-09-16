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

package io.kyligence.kap.smart.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import io.kyligence.kap.smart.common.AbstractContext;
import io.kyligence.kap.smart.util.OLAPContextUtil;
import io.kyligence.kap.smart.util.TableAliasGenerator;

public class ModelContext extends AbstractContext {

    private String modelName;

    private String project;
    private TableDesc rootFactTable = null;
    private Map<String, OLAPContext> contexts = new HashMap<>();

    private TableAliasGenerator.TableAliasDict dict;
    private Map<TableRef, String> tableRefAlias;

    public ModelContext(KylinConfig kylinConfig, String project, TableDesc rootFactTable) {
        super(kylinConfig);
        this.project = project;
        this.rootFactTable = rootFactTable;
        Map<String, TableDesc> tableMap = MetadataManager.getInstance(kylinConfig).getAllTablesMap(project);
        this.dict = TableAliasGenerator.generateNewDict(tableMap.keySet().toArray(new String[0]));
        this.tableRefAlias = new HashMap<>();
    }

    public TableDesc getRootTable() {
        return rootFactTable;
    }

    public void addContext(String sql, OLAPContext ctx) {
        this.contexts.put(sql, ctx);
        this.tableRefAlias.putAll(getTableAliasMap(ctx, dict));
    }

    public Collection<String> getAllQueries() {
        return contexts.keySet();
    }

    public Collection<OLAPContext> getOLAPContexts() {
        return contexts.values();
    }

    public OLAPContext getOLAPContext(String sql) {
        return contexts.get(sql);
    }

    public Map<TableRef, String> getAllTableRefAlias() {
        return this.tableRefAlias;
    }

    public String getTableRefAlias(TableRef tableRef) {
        return tableRefAlias.get(tableRef);
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    private static Map<TableRef, String> getTableAliasMap(OLAPContext ctx, TableAliasGenerator.TableAliasDict dict) {
        JoinsTree joinsTree = ctx.joinsTree;
        if (joinsTree == null) {
            joinsTree = new JoinsTree(ctx.firstTableScan.getTableRef(), ctx.joins);
        }

        Map<TableRef, String> allTableAlias = new HashMap<>();
        TableRef[] allTables = OLAPContextUtil.getAllTableRef(ctx);

        for (TableRef tableRef : allTables) {
            TableRef[] joinHierachy = getJoinHierchy(joinsTree, tableRef);
            String[] tableNames = new String[joinHierachy.length];

            for (int i = 0; i < joinHierachy.length; i++) {
                TableRef table = joinHierachy[i];
                tableNames[i] = table.getTableIdentity();
            }

            String tblAlias = (joinHierachy.length == 1 && joinHierachy[0] == ctx.firstTableScan.getTableRef())
                    ? ctx.firstTableScan.getTableRef().getTableName()
                    : dict.getHierachyAlias(tableNames);

            allTableAlias.put(tableRef, tblAlias);
        }
        return allTableAlias;
    }

    private static TableRef[] getJoinHierchy(JoinsTree joinsTree, TableRef leaf) {
        if (leaf == null) {
            return new TableRef[0];
        }

        JoinDesc join = joinsTree.getJoinByPKSide(leaf);
        if (join == null) {
            return new TableRef[] { leaf };
        }

        return (TableRef[]) ArrayUtils.add(getJoinHierchy(joinsTree, join.getFKSide()), leaf);
    }
}