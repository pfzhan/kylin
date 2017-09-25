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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    private Map<String, Collection<OLAPContext>> contexts = new HashMap<>();

    private TableAliasGenerator.TableAliasDict dict;
    private Map<TableRef, String> innerTableRefAlias;
    private Map<TableRef, String> correctedTableAlias;

    public ModelContext(KylinConfig kylinConfig, String project, TableDesc rootFactTable) {
        super(kylinConfig);
        this.project = project;
        this.rootFactTable = rootFactTable;
        Map<String, TableDesc> tableMap = MetadataManager.getInstance(kylinConfig).getAllTablesMap(project);
        this.dict = TableAliasGenerator.generateNewDict(tableMap.keySet().toArray(new String[0]));
        this.innerTableRefAlias = new HashMap<>();
        this.correctedTableAlias = new HashMap<>();
    }

    public TableDesc getRootTable() {
        return rootFactTable;
    }

    public void addContext(String sql, OLAPContext ctx) {
        if (!this.contexts.containsKey(sql)) {
            this.contexts.put(sql, new ArrayList<OLAPContext>());
        }
        this.contexts.get(sql).add(ctx);
        this.innerTableRefAlias.putAll(getTableAliasMap(ctx, dict));
        correctTableAlias();
    }

    private void correctTableAlias() {
        Map<String, TableDesc> classifiedAlias = new HashMap<>();
        for (Entry<TableRef, String> entry : innerTableRefAlias.entrySet()) {
            classifiedAlias.put(entry.getValue(), entry.getKey().getTableDesc());
        }
        Map<String, String> orig2corrected = new HashMap<>();
        // correct fact table alias in 1st place
        String factTableName = rootFactTable.getName();
        orig2corrected.put(factTableName, factTableName);
        classifiedAlias.remove(factTableName);
        for (Entry<String, TableDesc> entry : classifiedAlias.entrySet()) {
            String original = entry.getKey();
            String tableName = entry.getValue().getName();
            String corrected = tableName;
            int i = 1;
            while (orig2corrected.containsValue(corrected)) {
                corrected = tableName + "_" + i;
                i++;
            }
            orig2corrected.put(original, corrected);
        }
        for (Entry<TableRef, String> entry : innerTableRefAlias.entrySet()) {
            String corrected = orig2corrected.get(entry.getValue());
            correctedTableAlias.put(entry.getKey(), corrected);
        }
    }

    public Collection<String> getAllQueries() {
        return contexts.keySet();
    }

    public Collection<OLAPContext> getAllOLAPContexts() {
        List<OLAPContext> result = new ArrayList<>();
        for (Collection<OLAPContext> contextsBySQL : contexts.values()) {
            result.addAll(contextsBySQL);
        }
        return result;
    }

    public Collection<OLAPContext> getOLAPContext(String sql) {
        return contexts.get(sql);
    }

    public Map<TableRef, String> getAllTableRefAlias() {
        return this.correctedTableAlias;
    }

    public String getTableRefAlias(TableRef tableRef) {
        return correctedTableAlias.get(tableRef);
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