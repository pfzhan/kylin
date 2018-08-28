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

package io.kyligence.kap.metadata.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.JoinsTree.Chain;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public  class NDataModel extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(NDataModel.class);

    public static enum TableKind implements Serializable {
        FACT, LOOKUP
    }

    public static enum RealizationCapacity implements Serializable {
        SMALL, MEDIUM, LARGE
    }

    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("is_draft")
    private boolean isDraft;

    @JsonProperty("description")
    private String description;

    @JsonProperty("fact_table")
    private String rootFactTable;

    @JsonProperty("lookups")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private JoinTableDesc[] joinTables;

    @JsonProperty("join_tables")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private JoinTableDesc[] deprecatedLookups; // replaced by "join_tables" since KYLIN-1875

    @JsonProperty("dimensions")
    private List<ModelDimensionDesc> dimensions;

    @JsonProperty("metrics")
    private String[] metrics;

    @JsonProperty("filter_condition")
    private String filterCondition;

    @JsonProperty("partition_desc")
    PartitionDesc partitionDesc;

    @JsonProperty("capacity")
    private RealizationCapacity capacity = RealizationCapacity.MEDIUM;

    // computed attributes
    private TableRef rootFactTableRef;
    private Set<TableRef> factTableRefs = Sets.newLinkedHashSet();
    private Set<TableRef> lookupTableRefs = Sets.newLinkedHashSet();
    private Set<TableRef> allTableRefs = Sets.newLinkedHashSet();
    private Map<String, TableRef> aliasMap = Maps.newHashMap(); // alias => TableRef, a table has exactly one alias
    private Map<String, TableRef> tableNameMap = Maps.newHashMap(); // name => TableRef, a table maybe referenced by multiple names
    private JoinsTree joinsTree;

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<String>();


    public KylinConfig getConfig() {
        return config;
    }

    @Override
    public String resourceName() {
        return name;
    }

    public String getName() {
        return name;
    }

    // for updating name from draft to ready
    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isDraft() {
        return isDraft;
    }

    public void setDraft(boolean isDraft) {
        this.isDraft = isDraft;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public TableRef getRootFactTable() {
        return rootFactTableRef;
    }

    public String getRootFactTableName() {
        return rootFactTable;
    }

    public void setRootFactTableName(String rootFactTable) {
        this.rootFactTable = rootFactTable;
    }

    public Set<TableRef> getAllTables() {
        return allTableRefs;
    }

    public Set<TableRef> getFactTables() {
        return factTableRefs;
    }

    public Map<String, TableRef> getAliasMap() {
        return Collections.unmodifiableMap(aliasMap);
    }

    public Set<TableRef> getLookupTables() {
        return lookupTableRefs;
    }

    public JoinTableDesc[] getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(JoinTableDesc[] joinTables) {
        this.joinTables = joinTables;
    }

    public JoinDesc getJoinByPKSide(TableRef table) {
        return joinsTree.getJoinByPKSide(table);
    }

    public JoinsTree getJoinsTree() {
        return joinsTree;
    }

    @Deprecated
    public List<TableDesc> getLookupTableDescs() {
        List<TableDesc> result = Lists.newArrayList();
        for (TableRef table : getLookupTables()) {
            result.add(table.getTableDesc());
        }
        return result;
    }

    public boolean isLookupTable(TableRef t) {
        if (t == null)
            return false;
        else
            return lookupTableRefs.contains(t);
    }

    public boolean isLookupTable(String fullTableName) {
        for (TableRef t : lookupTableRefs) {
            if (t.getTableIdentity().equals(fullTableName))
                return true;
        }
        return false;
    }

    public boolean isFactTable(TableRef t) {
        if (t == null)
            return false;
        else
            return factTableRefs.contains(t);
    }

    //TODO: different from isFactTable(TableRef t)
    public boolean isFactTable(String fullTableName) {
        for (TableRef t : factTableRefs) {
            if (t.getTableIdentity().equals(fullTableName))
                return true;
        }
        return false;
    }

    public boolean isRootFactTable(TableDesc table) {
        if (table == null || StringUtils.isBlank(table.getIdentity())
                || StringUtils.isBlank(table.getProject())) {
            return false;
        }

        return rootFactTableRef.getTableIdentity().equals(table.getIdentity())
                && rootFactTableRef.getTableDesc().getProject().equals(table.getProject());
    }

    public boolean containsTable(TableDesc table) {
        for (TableRef t : allTableRefs) {
            if (t.getTableIdentity().equals(table.getIdentity())
                    && StringUtil.equals(t.getTableDesc().getProject(), table.getProject()))
                return true;
        }
        return false;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public RealizationCapacity getCapacity() {
        return capacity;
    }

    public void setCapacity(RealizationCapacity capacity) {
        this.capacity = capacity;
    }

    public TblColRef findColumn(String table, String column) throws IllegalArgumentException {
        TableRef tableRef = findTable(table);
        TblColRef result = tableRef.getColumn(column.toUpperCase());
        if (result == null)
            throw new IllegalArgumentException("Column not found by " + table + "." + column);
        return result;
    }

    public TblColRef findColumn(String column) throws IllegalArgumentException {
        TblColRef result = null;
        String input = column;

        column = column.toUpperCase();
        int cut = column.lastIndexOf('.');
        if (cut > 0) {
            // table specified
            result = findColumn(column.substring(0, cut), column.substring(cut + 1));
        } else {
            // table not specified, try each table
            for (TableRef tableRef : allTableRefs) {
                result = tableRef.getColumn(column);
                if (result != null)
                    break;
            }
        }

        if (result == null)
            throw new IllegalArgumentException("Column not found by " + input);

        return result;
    }

    // find by unique name, that must uniquely identifies a table in the model
    public TableRef findTable(String table) throws IllegalArgumentException {
        TableRef result = tableNameMap.get(table.toUpperCase());
        if (result == null) {
            throw new IllegalArgumentException("Table not found by " + table);
        }
        return result;
    }

    // find by table identity, that may match multiple tables in the model
    public TableRef findFirstTable(String tableIdentity) throws IllegalArgumentException {
        if (rootFactTableRef.getTableIdentity().equals(tableIdentity))
            return rootFactTableRef;

        for (TableRef fact : factTableRefs) {
            if (fact.getTableIdentity().equals(tableIdentity))
                return fact;
        }

        for (TableRef lookup : lookupTableRefs) {
            if (lookup.getTableIdentity().equals(tableIdentity))
                return lookup;
        }
        throw new IllegalArgumentException("Table not found by " + tableIdentity + " in model " + name);
    }

    public void init(KylinConfig config, Map<String, TableDesc> tables, boolean isOnlineModel) {
        this.config = config;

        initJoinTablesForUpgrade();
        initTableAlias(tables);
        initJoinColumns();
        reorderJoins(tables);
        initJoinsTree();
        initDimensionsAndMetrics();
        initPartitionDesc();
        initFilterCondition();

        boolean reinit = validate();
        if (reinit) { // model slightly changed by validate() and must init() again
            init(config, tables, isOnlineModel);
        }
    }

    private void initJoinTablesForUpgrade() {
        if (joinTables == null) {
            joinTables = new JoinTableDesc[0];
        }
        if (deprecatedLookups != null) {
            JoinTableDesc[] copy = Arrays.copyOf(joinTables, joinTables.length + deprecatedLookups.length);
            System.arraycopy(deprecatedLookups, 0, copy, joinTables.length, deprecatedLookups.length);
            joinTables = copy;
            deprecatedLookups = null;
        }
    }

    private void initTableAlias(Map<String, TableDesc> tables) {
        factTableRefs.clear();
        lookupTableRefs.clear();
        allTableRefs.clear();
        aliasMap.clear();
        tableNameMap.clear();

        if (StringUtils.isEmpty(rootFactTable)) {
            throw new IllegalStateException("root fact table should not be empty");
        }

        rootFactTable = rootFactTable.toUpperCase();
        if (tables.containsKey(rootFactTable) == false)
            throw new IllegalStateException("Root fact table does not exist:" + rootFactTable);

        TableDesc rootDesc = tables.get(rootFactTable);
        rootFactTableRef = new TableRef(this, rootDesc.getName(), rootDesc, false);

        addAlias(rootFactTableRef);
        factTableRefs.add(rootFactTableRef);

        for (JoinTableDesc join : joinTables) {
            join.setTable(join.getTable().toUpperCase());

            if (tables.containsKey(join.getTable()) == false)
                throw new IllegalStateException("Join table does not exist:" + join.getTable());

            TableDesc tableDesc = tables.get(join.getTable());
            String alias = join.getAlias();
            if (alias == null) {
                alias = tableDesc.getName();
            }
            alias = alias.toUpperCase();
            join.setAlias(alias);

            TableRef ref = new TableRef(this, alias, tableDesc, true);

            join.setTableRef(ref);
            addAlias(ref);
            (join.getKind() == TableKind.LOOKUP ? lookupTableRefs : factTableRefs).add(ref);
        }

        tableNameMap.putAll(aliasMap);
        allTableRefs.addAll(factTableRefs);
        allTableRefs.addAll(lookupTableRefs);
    }

    private void addAlias(TableRef ref) {
        String alias = ref.getAlias();
        if (aliasMap.containsKey(alias))
            throw new IllegalStateException("Alias '" + alias + "' ref to multiple tables: " + ref.getTableIdentity()
                    + ", " + aliasMap.get(alias).getTableIdentity());
        aliasMap.put(alias, ref);

        TableDesc table = ref.getTableDesc();
        addTableName(table.getName(), ref);
        addTableName(table.getIdentity(), ref);
    }

    private void addTableName(String name, TableRef ref) {
        if (tableNameMap.containsKey(name)) {
            tableNameMap.put(name, null); // conflict name
        } else {
            tableNameMap.put(name, ref);
        }
    }

    private void initDimensionsAndMetrics() {
        if (dimensions == null)
            dimensions = new ArrayList<>(0);
        if (metrics == null)
            metrics = new String[0];

        for (ModelDimensionDesc dim : dimensions) {
            dim.init(this);
        }
        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = findColumn(metrics[i]).getIdentity();
        }
    }

    private void initPartitionDesc() {
        if (this.partitionDesc != null)
            this.partitionDesc.init(this);
    }

    //Check if the filter condition is illegal.
    private void initFilterCondition() {
        if (null == this.filterCondition) {
            return;
        }
        int quotationType = 0;
        int len = this.filterCondition.length();
        for (int i = 0; i < len; i++) {
            //If a ';' which is not within a string is found, throw exception.
            if (';' == this.filterCondition.charAt(i) && 0 == quotationType) {
                throw new IllegalStateException(
                        "Filter Condition is Illegal. Please check it and make sure it's an appropriate expression for WHERE clause");
            }
            if ('\'' == this.filterCondition.charAt(i)) {
                if (quotationType > 0) {
                    if (1 == quotationType) {
                        quotationType = 0;
                        continue;
                    }
                } else {
                    if (0 == quotationType) {
                        quotationType = 1;
                        continue;
                    }
                }
            }
            if ('"' == this.filterCondition.charAt(i)) {
                if (quotationType > 0) {
                    if (2 == quotationType) {
                        quotationType = 0;
                        continue;
                    }
                } else {
                    if (0 == quotationType) {
                        quotationType = 2;
                        continue;
                    }
                }
            }
        }
    }

    private void initJoinColumns() {

        for (JoinTableDesc joinTable : joinTables) {
            TableRef dimTable = joinTable.getTableRef();
            JoinDesc join = joinTable.getJoin();
            if (join == null)
                throw new IllegalStateException("Missing join conditions on table " + dimTable);

            StringUtil.toUpperCaseArray(join.getForeignKey(), join.getForeignKey());
            StringUtil.toUpperCaseArray(join.getPrimaryKey(), join.getPrimaryKey());

            // primary key
            String[] pks = join.getPrimaryKey();
            TblColRef[] pkCols = new TblColRef[pks.length];
            for (int i = 0; i < pks.length; i++) {
                TblColRef col = dimTable.getColumn(pks[i]);
                if (col == null) {
                    col = findColumn(pks[i]);
                }
                if (col == null || col.getTableRef().equals(dimTable) == false) {
                    throw new IllegalStateException("Can't find PK column " + pks[i] + " in table " + dimTable);
                }
                pks[i] = col.getIdentity();
                pkCols[i] = col;
            }
            join.setPrimaryKeyColumns(pkCols);

            // foreign key
            String[] fks = join.getForeignKey();
            TblColRef[] fkCols = new TblColRef[fks.length];
            for (int i = 0; i < fks.length; i++) {
                TblColRef col = findColumn(fks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find FK column " + fks[i]);
                }
                fks[i] = col.getIdentity();
                fkCols[i] = col;
            }
            join.setForeignKeyColumns(fkCols);

            join.sortByFK();

            // Validate join in dimension
            TableRef fkTable = fkCols[0].getTableRef();
            if (pkCols.length == 0 || fkCols.length == 0)
                throw new IllegalStateException("Missing join columns on table " + dimTable);
            if (pkCols.length != fkCols.length) {
                throw new IllegalStateException("Primary keys(" + dimTable + ")" + Arrays.toString(pks)
                        + " are not consistent with Foreign keys(" + fkTable + ") " + Arrays.toString(fks));
            }
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    logger.warn("PK " + dimTable + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype()
                            + " are not consistent with FK " + fkTable + "." + fkCols[i].getName() + "."
                            + fkCols[i].getDatatype());
                }
            }
        }
    }

    private void initJoinsTree() {
        List<JoinDesc> joins = new ArrayList<>();
        for (JoinTableDesc joinTable : joinTables) {
            joins.add(joinTable.getJoin());
        }
        joinsTree = new JoinsTree(rootFactTableRef, joins);
    }

    private void reorderJoins(Map<String, TableDesc> tables) {
        if (joinTables.length == 0) {
            return;
        }

        Map<String, List<JoinTableDesc>> fkMap = Maps.newHashMap();
        for (JoinTableDesc joinTable : joinTables) {
            JoinDesc join = joinTable.getJoin();
            String fkSideName = join.getFKSide().getAlias();
            if (fkMap.containsKey(fkSideName)) {
                fkMap.get(fkSideName).add(joinTable);
            } else {
                List<JoinTableDesc> joinTableList = Lists.newArrayList();
                joinTableList.add(joinTable);
                fkMap.put(fkSideName, joinTableList);
            }
        }

        JoinTableDesc[] orderedJoinTables = new JoinTableDesc[joinTables.length];
        int orderedIndex = 0;

        Queue<JoinTableDesc> joinTableBuff = new ArrayDeque<JoinTableDesc>();
        TableDesc rootDesc = tables.get(rootFactTable);
        joinTableBuff.addAll(fkMap.get(rootDesc.getName()));
        while (!joinTableBuff.isEmpty()) {
            JoinTableDesc head = joinTableBuff.poll();
            orderedJoinTables[orderedIndex++] = head;
            String headAlias = head.getJoin().getPKSide().getAlias();
            if (fkMap.containsKey(headAlias)) {
                joinTableBuff.addAll(fkMap.get(headAlias));
            }
        }

        joinTables = orderedJoinTables;
    }

    private boolean validate() {

        // ensure no dup between dimensions/metrics
        for (ModelDimensionDesc dim : dimensions) {
            String table = dim.getTable();
            for (String c : dim.getColumns()) {
                TblColRef dcol = findColumn(table, c);
                metrics = ArrayUtils.removeElement(metrics, dcol.getIdentity());
            }
        }

        Set<TblColRef> mcols = new HashSet<>();
        for (String m : metrics) {
            mcols.add(findColumn(m));
        }

        // validate PK/FK are in dimensions
        boolean pkfkDimAmended = false;
        for (Chain chain : joinsTree.tableChains.values()) {
            pkfkDimAmended = validatePkFkDim(chain.join, mcols) || pkfkDimAmended;
        }
        return pkfkDimAmended;
    }

    private boolean validatePkFkDim(JoinDesc join, Set<TblColRef> mcols) {
        if (join == null)
            return false;

        boolean pkfkDimAmended = false;

        for (TblColRef c : join.getForeignKeyColumns()) {
            if (!mcols.contains(c)) {
                pkfkDimAmended = validatePkFkDim(c) || pkfkDimAmended;
            }
        }
        for (TblColRef c : join.getPrimaryKeyColumns()) {
            if (!mcols.contains(c)) {
                pkfkDimAmended = validatePkFkDim(c) || pkfkDimAmended;
            }
        }
        return pkfkDimAmended;
    }

    private boolean validatePkFkDim(TblColRef c) {
        String t = c.getTableAlias();
        ModelDimensionDesc dimDesc = null;
        for (ModelDimensionDesc dim : dimensions) {
            if (dim.getTable().equals(t)) {
                dimDesc = dim;
                break;
            }
        }

        if (dimDesc == null) {
            dimDesc = new ModelDimensionDesc();
            dimDesc.setTable(t);
            dimDesc.setColumns(new String[0]);
            dimensions.add(dimDesc);
        }

        if (ArrayUtils.contains(dimDesc.getColumns(), c.getName()) == false) {
            String[] newCols = ArrayUtils.add(dimDesc.getColumns(), c.getName());
            dimDesc.setColumns(newCols);
            return true;
        }

        return false;
    }

    public boolean isStandardPartitionedDateColumn() {
        if (StringUtils.isBlank(getPartitionDesc().getPartitionDateFormat())) {
            return false;
        }
        return true;
    }

    /**
     * Add error info and thrown exception out
     */
    public void addError(String message) {
        addError(message, false);
    }

    /**
     * @param message error message
     * @param silent  if throw exception
     */
    public void addError(String message, boolean silent) {
        if (!silent) {
            throw new IllegalStateException(message);
        } else {
            this.errors.add(message);
        }
    }

    public List<String> getError() {
        return this.errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NDataModel modelDesc = (NDataModel) o;

        if (!name.equals(modelDesc.name))
            return false;
        if (!getRootFactTable().equals(modelDesc.getRootFactTable()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + name.hashCode();
        result = 31 * result + getRootFactTable().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "NDataModel [name=" + name + "]";
    }

    public static String concatResourcePath(String descName) {
        return ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public List<ModelDimensionDesc> getDimensions() {
        return dimensions;
    }

    public String[] getMetrics() {
        return metrics;
    }

    public void setDimensions(List<ModelDimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public void setMetrics(String[] metrics) {
        this.metrics = metrics;
    }

    public ProjectInstance getProjectInstance() {
        return NProjectManager.getInstance(getConfig()).getProject(project);
    }

    public static NDataModel copy(NDataModel orig, NDataModel copy) {
        copy.config = orig.config;
        copy.name = orig.name;
        copy.isDraft = orig.isDraft;
        copy.owner = orig.owner;
        copy.description = orig.description;
        copy.rootFactTable = orig.rootFactTable;
        copy.joinTables = orig.joinTables;
        copy.dimensions = orig.dimensions;
        copy.metrics = orig.metrics;
        copy.filterCondition = orig.filterCondition;
        copy.capacity = orig.capacity;
        copy.lastModified = orig.lastModified;
        if (orig.getPartitionDesc() != null) {
            copy.partitionDesc = PartitionDesc.getCopyOf(orig.getPartitionDesc());
        }
        copy.updateRandomUuid();
        return copy;
    }





    // TODO: newten:

    public static final int MEASURE_ID_BASE = 1000;

    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    public static class NamedColumn implements Serializable, IKeep {
        @JsonProperty("id")
        public int id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("column")
        public String aliasDotColumn;
        // logical delete symbol
        @JsonProperty("tomb")
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public boolean tomb = false;
    }

    public static class Measure extends MeasureDesc implements IKeep {
        @JsonProperty("id")
        public int id;
        // logical delete symbol
        @JsonProperty("tomb")
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public boolean tomb = false;
    }

    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    public static class ColumnCorrelation implements Serializable, IKeep {
        @JsonProperty("name")
        public String name;
        @JsonProperty("correlation_type") // "hierarchy" or "joint"
        public String corrType;
        @JsonProperty("columns")
        public String[] aliasDotColumns;

        public TblColRef[] cols;
    }

    // ============================================================================

    @JsonProperty("all_named_columns")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<NamedColumn> allNamedColumns = new ArrayList<>(); // including deleted ones

    @JsonProperty("all_measures")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<Measure> allMeasures = new ArrayList<>(); // including deleted ones

    @JsonProperty("column_correlations")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<ColumnCorrelation> colCorrs = new ArrayList<>();

    @JsonProperty("multilevel_partition_cols")
    @JsonInclude(JsonInclude.Include.NON_NULL) // output to frontend
    private String[] mpColStrs = new String[0];

    @JsonProperty("computed_columns")
    @JsonInclude(JsonInclude.Include.NON_NULL) // output to frontend
    private List<ComputedColumnDesc> computedColumnDescs = Lists.newArrayList();

    // computed fields below
    private String project;
    private List<TblColRef> allCols; // including DELETED cols
    private ImmutableBiMap<Integer, TblColRef> effectiveCols; // excluding DELETED cols
    private ImmutableBiMap<Integer, Measure> effectiveMeasures; // excluding DELETED cols
    //private Map<TableRef, BitSet> effectiveDerivedCols;
    private ImmutableMultimap<TblColRef, TblColRef> fk2Pk;
    private TblColRef[] mpCols;

    // don't use unless you're sure, for jackson only
    public NDataModel() {
        super();
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<NDataModel> otherModels,
                     boolean isOnlineModel) {
        // tweak the tables according to Computed Columns defined in model
        Map<String, TableDesc> tables = Maps.newHashMap();
        for (Map.Entry<String, TableDesc> entry : originalTables.entrySet()) {
            String s = entry.getKey();
            TableDesc tableDesc = entry.getValue();

            // null is possible when only involved table metadata is copied to remote executor
            if (tableDesc == null)
                continue;

            TableDesc extendedTableDesc = tableDesc.appendColumns(createComputedColumns(tableDesc), !isOnlineModel);
            tables.put(s, extendedTableDesc);
        }

        init(config, tables, isOnlineModel);

        initComputedColumns(otherModels);
        initMultilevelPartitionCols();
        initAllNamedColumns();
        initAllMeasures();
        initFk2Pk();
    }

    private void initAllNamedColumns() {
        List<TblColRef> all = new ArrayList<>(allNamedColumns.size());
        ImmutableBiMap.Builder<Integer, TblColRef> mapBuilder = ImmutableBiMap.builder();
        for (NamedColumn d : allNamedColumns) {
            TblColRef col = this.findColumn(d.aliasDotColumn);
            d.aliasDotColumn = col.getIdentity();
            all.add(col);

            if (!d.tomb) {
                mapBuilder.put(d.id, col);
            }
        }

        this.allCols = all;
        this.effectiveCols = mapBuilder.build();

        checkNoDup(effectiveCols);
    }

    private <T> void checkNoDup(ImmutableBiMap<Integer, T> idMap) {
        Map<T, Integer> reverseMap = new HashMap<>();
        for (Map.Entry<Integer, T> e : idMap.entrySet()) {
            int id = e.getKey();
            T value = e.getValue();
            if (reverseMap.containsKey(value)) {
                throw new IllegalStateException("Illegal model '" + getName() + "', " + value + " has duplicated ID: "
                        + reverseMap.get(value) + " and " + id);
            }
            reverseMap.put(value, id);
        }
    }

    private void initAllMeasures() {
        List<Measure> all = new ArrayList<>(allMeasures.size());
        ImmutableBiMap.Builder<Integer, Measure> mapBuilder = ImmutableBiMap.builder();
        for (Measure m : allMeasures) {
            m.setName(m.getName().toUpperCase());
            FunctionDesc func = m.getFunction();
            func.init(this);
            all.add(m);

            if (!m.tomb) {
                mapBuilder.put(m.id, m);
            }
        }

        this.allMeasures = all;
        this.effectiveMeasures = mapBuilder.build();
        checkNoDupAndEffective(effectiveMeasures);
    }

    private void initFk2Pk() {
        ImmutableMultimap.Builder<TblColRef, TblColRef> builder = ImmutableMultimap.builder();
        for (JoinTableDesc joinTable : this.getJoinTables()) {
            JoinDesc join = joinTable.getJoin();
            int n = join.getForeignKeyColumns().length;
            for (int i = 0; i < n; i++) {
                TblColRef pk = join.getPrimaryKeyColumns()[i];
                TblColRef fk = join.getForeignKeyColumns()[i];
                builder.put(fk, pk);
            }
        }
        this.fk2Pk = builder.build();
    }

    private void checkNoDupAndEffective(ImmutableBiMap<Integer, Measure> effectiveMeasures) {
        checkNoDup(effectiveMeasures);

        // check there is one count()
        int countNum = 0;
        for (MeasureDesc m : effectiveMeasures.values()) {
            if (m.getFunction().isCount())
                countNum++;
        }
        if (countNum != 1)
            throw new IllegalStateException("Illegal model '" + getName()
                    + "', should have one and only one COUNT() measure but there are " + countNum);

        // check all measure columns are effective
        for (MeasureDesc m : effectiveMeasures.values()) {
            List<TblColRef> mCols = m.getFunction().getParameter().getColRefs();
            if (effectiveCols.values().containsAll(mCols) == false) {
                List<TblColRef> notEffective = new ArrayList<>(mCols);
                notEffective.removeAll(effectiveCols.values());
                throw new IllegalStateException("Illegal model '" + getName() + "', some columns referenced in " + m
                        + " is not on model: " + notEffective);
            }
        }
    }

    List<TblColRef> getAllCols() {
        return allCols;
    }

    public List<Measure> getAllMeasures() {
        return allMeasures;
    }

    /**
     * returns ID <==> TblColRef
     */
    public ImmutableBiMap<Integer, TblColRef> getEffectiveColsMap() {
        return effectiveCols;
    }

    /**
     * returns ID <==> Measure
     */
    public ImmutableBiMap<Integer, Measure> getEffectiveMeasureMap() {
        return effectiveMeasures;
    }

    //TODO: !!! check the returned
    public @Nullable
    Integer getColId(TblColRef colRef) {
        return effectiveCols.inverse().get(colRef);
    }

    public boolean isExtendedColumn(TblColRef tblColRef) {
        return false; // TODO: enable derived
    }

    private void initMultilevelPartitionCols() {
        mpCols = new TblColRef[mpColStrs.length];
        if (mpColStrs.length == 0)
            return;

        StringUtil.toUpperCaseArray(mpColStrs, mpColStrs);

        for (int i = 0; i < mpColStrs.length; i++) {
            mpCols[i] = findColumn(mpColStrs[i]);
            mpColStrs[i] = mpCols[i].getIdentity();

            DataType type = mpCols[i].getType();
            if (!type.isNumberFamily() && !type.isStringFamily())
                throw new IllegalStateException(
                        "Multi-level partition column must be Number or String, but " + mpCols[i] + " is " + type);
        }

        checkMPColsBelongToModel(mpCols);
    }

    private void checkMPColsBelongToModel(TblColRef[] tcr) {
        Set<TblColRef> refSet = effectiveCols.values();
        if (!refSet.containsAll(Sets.newHashSet(tcr))) {
            throw new IllegalStateException("Primary partition column should inside of this model.");
        }
    }

    private void initComputedColumns(List<NDataModel> otherModels) {
        Preconditions.checkNotNull(otherModels);

        List<Pair<ComputedColumnDesc, NDataModel>> existingCCs = Lists.newArrayList();

        for (NDataModel dataModelDesc : otherModels) {
            if (dataModelDesc instanceof NDataModel) {
                NDataModel otherModel = (NDataModel) dataModelDesc;
                if (!StringUtils.equals(otherModel.getName(), this.getName())) {
                    for (ComputedColumnDesc cc : otherModel.getComputedColumnDescs()) {
                        existingCCs.add(Pair.newPair(cc, otherModel));
                    }
                }
            }
        }

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {

            newCC.init(getAliasMap(), getRootFactTable().getAlias());
            final String newCCFullName = newCC.getFullName();
            final String newCCColumnName = newCC.getColumnName();

            for (Pair<ComputedColumnDesc, NDataModel> pair : existingCCs) {
                NDataModel dataModelDesc = pair.getSecond();
                ComputedColumnDesc cc = pair.getFirst();

                if (StringUtils.equalsIgnoreCase(cc.getFullName(), newCCFullName) && !(cc.equals(newCC))) {
                    throw new IllegalArgumentException(String.format(
                            "Column name for computed column %s is already used in model %s, you should apply the same expression ' %s ' here, or use a different column name.",
                            newCCFullName, dataModelDesc.getName(), cc.getExpression()));
                }

                if (isTwoCCDefinitionEquals(cc.getExpression(), newCC.getExpression())
                        && !StringUtils.equalsIgnoreCase(cc.getColumnName(), newCCColumnName)) {
                    throw new IllegalArgumentException(String.format(
                            "Expression %s in computed column %s is already defined by computed column %s from model %s, you should use the same column name: ' %s ' .",
                            newCC.getExpression(), newCCFullName, cc.getFullName(), dataModelDesc.getName(),
                            cc.getColumnName()));
                }
            }
            existingCCs.add(Pair.newPair(newCC, this));
        }
    }

    private boolean isTwoCCDefinitionEquals(String definition0, String definition1) {
        definition0 = definition0.replaceAll("\\s*", "");
        definition1 = definition1.replaceAll("\\s*", "");
        return definition0.equalsIgnoreCase(definition1);
    }

    private ColumnDesc[] createComputedColumns(final TableDesc tableDesc) {
        final MutableInt id = new MutableInt(tableDesc.getColumnCount());
        return FluentIterable.from(this.computedColumnDescs).filter(new Predicate<ComputedColumnDesc>() {
            @Override
            public boolean apply(@Nullable ComputedColumnDesc input) {
                return tableDesc.getIdentity().equalsIgnoreCase(input.getTableIdentity());
            }
        }).transform(new Function<ComputedColumnDesc, ColumnDesc>() {
            @Nullable
            @Override
            public ColumnDesc apply(@Nullable ComputedColumnDesc input) {
                id.increment();
                ColumnDesc columnDesc = new ColumnDesc(id.toString(), input.getColumnName(), input.getDatatype(),
                        input.getComment(), null, null, input.getExpression());
                return columnDesc;
            }
        }).toArray(ColumnDesc.class);
    }

    public ComputedColumnDesc findCCByCCColumnName(final String columnName) {
        return Iterables.find(this.computedColumnDescs, new Predicate<ComputedColumnDesc>() {
            @Override
            public boolean apply(@Nullable ComputedColumnDesc input) {
                Preconditions.checkNotNull(input);
                return columnName.equals(input.getColumnName());
            }
        });
    }

    public List<ComputedColumnDesc> getComputedColumnDescs() {
        return computedColumnDescs;
    }

    public void setComputedColumnDescs(List<ComputedColumnDesc> computedColumnDescs) {
        this.computedColumnDescs = computedColumnDescs;
    }

    public Set<String> getComputedColumnNames() {
        Set<String> ccColumnNames = Sets.newHashSet();
        for (ComputedColumnDesc cc : this.getComputedColumnDescs()) {
            ccColumnNames.add(cc.getColumnName());
        }
        return Collections.unmodifiableSet(ccColumnNames);
    }

    public boolean isTimePartitioned() {
        return getPartitionDesc().isPartitioned();
    }

    public boolean isMultiLevelPartitioned() {
        return mpColStrs.length > 0;
    }

    public String[] getMutiLevelPartitionColStrs() {
        return mpColStrs;
    }

    void setMutiLevelPartitionColStrs(String[] colStrs) {
        this.mpColStrs = colStrs;
    }

    public TblColRef[] getMutiLevelPartitionCols() {
        return mpCols;
    }

    public String[] getMpColStrs() {
        return mpColStrs;
    }

    void setMpColStrs(String[] mpColStrs) {
        this.mpColStrs = mpColStrs;
    }

    public List<NamedColumn> getAllNamedColumns() {
        return allNamedColumns;
    }

    public void setAllNamedColumns(List<NamedColumn> allNamedColumns) {
        this.allNamedColumns = allNamedColumns;
    }

    public void setAllMeasures(List<Measure> allMeasures) {
        this.allMeasures = allMeasures;
    }

    public List<ColumnCorrelation> getColCorrs() {
        return colCorrs;
    }

    void setColCorrs(List<ColumnCorrelation> colCorrs) {
        this.colCorrs = colCorrs;
    }

    public ImmutableMultimap<TblColRef, TblColRef> getFk2Pk() {
        return fk2Pk;
    }

    public int getColumnIdByColumnName(String aliasDotName) {
        Preconditions.checkArgument(allNamedColumns != null);
        for (NamedColumn col : allNamedColumns) {
            if (col.aliasDotColumn.equalsIgnoreCase(aliasDotName))
                return col.id;
        }
        return -1;
    }

    public String getColumnNameByColumnId(int id) {
        Preconditions.checkArgument(allNamedColumns != null);
        for (NamedColumn col : allNamedColumns) {
            if (col.id == id)
                return col.aliasDotColumn;
        }
        return null;
    }

    public static NDataModel getCopyOf(NDataModel orig) {
        NDataModel copy = NDataModel.copy(orig, new NDataModel());
        copy.setDimensions(Lists.<ModelDimensionDesc> newArrayList());
        copy.setMetrics(new String[0]);
        copy.setProject(orig.getProject());
        copy.computedColumnDescs = orig.computedColumnDescs;
        copy.allCols = orig.allCols;
        copy.allMeasures = orig.allMeasures;
        copy.allNamedColumns = orig.allNamedColumns;
        copy.colCorrs = orig.colCorrs;
        copy.mpColStrs = orig.mpColStrs;
        copy.mpCols = orig.mpCols;
        return copy;
    }

    @Override
    public String getResourcePath() {
        return new StringBuilder().append("/").append(project).append(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                .append("/").append(getName()).append(MetadataConstants.FILE_SURFIX).toString();
    }


}
