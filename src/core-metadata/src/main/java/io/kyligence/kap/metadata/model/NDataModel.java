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

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.alias.AliasDeduce;
import io.kyligence.kap.metadata.model.alias.AliasMapping;
import io.kyligence.kap.metadata.model.alias.ExpressionComparator;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.val;

@Data
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class NDataModel extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(NDataModel.class);
    public static final int MEASURE_ID_BASE = 1000;

    public enum TableKind implements Serializable {
        FACT, LOOKUP
    }

    public enum RealizationCapacity implements Serializable {
        SMALL, MEDIUM, LARGE
    }

    private KylinConfig config;

    @EqualsAndHashCode.Include
    @JsonProperty("name")
    private String name;

    @EqualsAndHashCode.Include
    @JsonProperty("alias")
    private String alias;

    @EqualsAndHashCode.Include
    @JsonProperty("owner")
    private String owner;

    @EqualsAndHashCode.Include
    @JsonProperty("is_draft")
    private boolean isDraft;

    @EqualsAndHashCode.Include
    @JsonProperty("description")
    private String description;

    @EqualsAndHashCode.Include
    @JsonProperty("fact_table")
    private String rootFactTableName;

    @EqualsAndHashCode.Include
    @JsonProperty("fact_table_alias")
    private String rootFactTableAlias;

    @EqualsAndHashCode.Include
    @JsonProperty("management_type")
    private ManagementType managementType = ManagementType.TABLE_ORIENTED;

    @JsonProperty("join_tables")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<JoinTableDesc> joinTables;

    @EqualsAndHashCode.Include
    @JsonProperty("filter_condition")
    private String filterCondition;

    @EqualsAndHashCode.Include
    @JsonProperty("partition_desc")
    private PartitionDesc partitionDesc;

    @EqualsAndHashCode.Include
    @JsonProperty("capacity")
    private RealizationCapacity capacity = RealizationCapacity.MEDIUM;

    @EqualsAndHashCode.Include
    @JsonProperty("auto_merge_enabled")
    private boolean autoMergeEnabled = true;

    @EqualsAndHashCode.Include
    @JsonProperty("auto_merge_time_ranges")
    private List<AutoMergeTimeEnum> autoMergeTimeRanges = Lists.newArrayList(AutoMergeTimeEnum.WEEK,
            AutoMergeTimeEnum.MONTH);

    @JsonProperty("volatile_range")
    private VolatileRange volatileRange = new VolatileRange();

    @JsonProperty("data_check_desc")
    private DataCheckDesc dataCheckDesc;

    @JsonProperty("semantic_version")
    private int semanticVersion;

    // computed attributes
    @EqualsAndHashCode.Include
    @JsonProperty("all_named_columns")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<NamedColumn> allNamedColumns = new ArrayList<>(); // including deleted ones

    @EqualsAndHashCode.Include
    @JsonProperty("all_measures")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<Measure> allMeasures = new ArrayList<>(); // including deleted ones

    @EqualsAndHashCode.Include
    @JsonProperty("column_correlations")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<ColumnCorrelation> colCorrs = new ArrayList<>();

    @EqualsAndHashCode.Include
    @JsonProperty("multilevel_partition_cols")
    @JsonInclude(JsonInclude.Include.NON_NULL) // output to frontend
    private List<String> mpColStrs = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonProperty("computed_columns")
    @JsonInclude(JsonInclude.Include.NON_NULL) // output to frontend
    private List<ComputedColumnDesc> computedColumnDescs = Lists.newArrayList();

    @JsonProperty("canvas")
    @JsonInclude(JsonInclude.Include.NON_NULL) // output to frontend
    private Canvas canvas;

    // computed fields below
    private String project;

    private ImmutableBiMap<Integer, TblColRef> effectiveCols; // excluding DELETED cols

    private ImmutableBiMap<Integer, TblColRef> effectiveDimensions; // including DIMENSION cols

    private ImmutableBiMap<Integer, Measure> effectiveMeasures; // excluding DELETED cols
    //private Map<TableRef, BitSet> effectiveDerivedCols;
    private ImmutableMultimap<TblColRef, TblColRef> fk2Pk;

    private List<TblColRef> mpCols;

    private TableRef rootFactTableRef;

    private Set<TableRef> factTableRefs = Sets.newLinkedHashSet();

    private Set<TableRef> lookupTableRefs = Sets.newLinkedHashSet();

    private Set<TableRef> allTableRefs = Sets.newLinkedHashSet();

    private Map<String, TableRef> aliasMap = Maps.newHashMap(); // alias => TableRef, a table has exactly one alias

    private Map<String, TableRef> tableNameMap = Maps.newHashMap(); // name => TableRef, a table maybe referenced by multiple names

    private JoinsGraph joinsGraph;

    // when set true, cc expression will allow null value
    private boolean isSeekingCCAdvice = false;

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<>();

    public enum ColumnStatus {
        TOMB, EXIST, DIMENSION
    }

    @Data
    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    @EqualsAndHashCode
    @ToString
    public static class NamedColumn implements Serializable, IKeep {
        @JsonProperty("id")
        private int id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("column")
        private String aliasDotColumn;

        // logical delete symbol
        @JsonProperty("status")
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private ColumnStatus status = ColumnStatus.EXIST;

        public boolean isExist() {
            return status != ColumnStatus.TOMB;
        }

        public boolean isDimension() {
            return status == ColumnStatus.DIMENSION;
        }
    }

    @EqualsAndHashCode
    public static class Measure extends MeasureDesc implements IKeep {
        @Getter
        @JsonProperty("id")
        public int id;
        // logical delete symbol
        @JsonProperty("tomb")
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public boolean tomb = false;

    }

    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    @EqualsAndHashCode
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

    // don't use unless you're sure(when in doubt, leave it out), for jackson only
    public NDataModel() {
        super();
    }

    public NDataModel(NDataModel other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.version = other.version;
        this.name = other.name;
        this.alias = other.alias;
        this.owner = other.owner;
        this.isDraft = other.isDraft;
        this.description = other.description;
        this.rootFactTableName = other.rootFactTableName;
        this.joinTables = other.joinTables;
        this.filterCondition = other.filterCondition;
        this.partitionDesc = other.partitionDesc;
        this.capacity = other.capacity;
        this.allNamedColumns = other.allNamedColumns;
        this.allMeasures = other.allMeasures;
        this.colCorrs = other.colCorrs;
        this.mpColStrs = other.mpColStrs;
        this.computedColumnDescs = other.computedColumnDescs;
        this.managementType = other.managementType;
        this.autoMergeEnabled = other.autoMergeEnabled;
        this.autoMergeTimeRanges = other.autoMergeTimeRanges;
        this.volatileRange = other.volatileRange;
        this.dataCheckDesc = other.dataCheckDesc;
        this.canvas = other.canvas;
    }

    public KylinConfig getConfig() {
        return config;
    }

    @Override
    public String resourceName() {
        return name;
    }

    public ManagementType getManagementType() {
        return managementType;
    }

    public void setManagementType(ManagementType managementType) {
        this.managementType = managementType;
    }

    public boolean isAutoMergeEnabled() {
        return autoMergeEnabled;
    }

    public void setAutoMergeEnabled(boolean autoMergeEnabled) {
        this.autoMergeEnabled = autoMergeEnabled;
    }

    public List<AutoMergeTimeEnum> getAutoMergeTimeRanges() {
        return autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(List<AutoMergeTimeEnum> autoMergeTimeRanges) {
        this.autoMergeTimeRanges = autoMergeTimeRanges;
    }

    public VolatileRange getVolatileRange() {
        return volatileRange;
    }

    public void setVolatileRange(VolatileRange volatileRange) {
        this.volatileRange = volatileRange;
    }

    public TableRef getRootFactTable() {
        return rootFactTableRef;
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

    public List<JoinTableDesc> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<JoinTableDesc> joinTables) {
        this.joinTables = joinTables;
    }

    public JoinDesc getJoinByPKSide(TableRef table) {
        return joinsGraph.getJoinByPKSide(table);
    }

    public JoinsGraph getJoinsGraph() {
        return joinsGraph;
    }

    public DataCheckDesc getDataCheckDesc() {
        if (dataCheckDesc == null) {
            return new DataCheckDesc();
        }
        return dataCheckDesc;
    }

    public void setDataCheckDesc(DataCheckDesc dataCheckDesc) {
        this.dataCheckDesc = dataCheckDesc;
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

    public boolean isJoinTable(String fullTableName) {
        if (joinTables == null) {
            return false;
        }
        for (val table : joinTables) {
            if (table.getTable().equals(fullTableName)) {
                return true;
            }
        }
        return false;
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
        if (table == null || StringUtils.isBlank(table.getIdentity()) || StringUtils.isBlank(table.getProject())) {
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

    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.config = config;

        initJoinTablesForUpgrade();
        initTableAlias(tables);
        initJoinColumns();
        reorderJoins(tables);
        initJoinsGraph();
        initPartitionDesc();
        initFilterCondition();
        if (StringUtils.isEmpty(this.alias)) {
            this.alias = this.name;
        }
    }

    private void initJoinTablesForUpgrade() {
        if (joinTables == null) {
            joinTables = Lists.newArrayList();
        }
    }

    private void initTableAlias(Map<String, TableDesc> tables) {
        factTableRefs.clear();
        lookupTableRefs.clear();
        allTableRefs.clear();
        aliasMap.clear();
        tableNameMap.clear();

        if (StringUtils.isEmpty(rootFactTableName)) {
            throw new IllegalStateException("root fact table should not be empty");
        }

        rootFactTableName = rootFactTableName.toUpperCase();
        if (tables.containsKey(rootFactTableName) == false)
            throw new IllegalStateException("Root fact table does not exist:" + rootFactTableName);

        TableDesc rootDesc = tables.get(rootFactTableName);
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

            boolean isLookup = join.getKind() == TableKind.LOOKUP;
            TableRef ref = new TableRef(this, alias, tableDesc, isLookup);

            join.setTableRef(ref);
            addAlias(ref);
            (isLookup ? lookupTableRefs : factTableRefs).add(ref);
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

    private void initJoinsGraph() {
        List<JoinDesc> joins = new ArrayList<>();
        for (JoinTableDesc joinTable : joinTables) {
            joins.add(joinTable.getJoin());
        }
        joinsGraph = new JoinsGraph(rootFactTableRef, joins);
    }

    private void reorderJoins(Map<String, TableDesc> tables) {
        if (CollectionUtils.isEmpty(joinTables)) {
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

        val orderedJoinTables = Arrays.asList(new JoinTableDesc[joinTables.size()]);
        int orderedIndex = 0;

        Queue<JoinTableDesc> joinTableBuff = new ArrayDeque<>();
        TableDesc rootDesc = tables.get(rootFactTableName);
        joinTableBuff.addAll(fkMap.get(rootDesc.getName()));
        while (!joinTableBuff.isEmpty()) {
            JoinTableDesc head = joinTableBuff.poll();
            orderedJoinTables.set(orderedIndex++, head);
            String headAlias = head.getJoin().getPKSide().getAlias();
            if (fkMap.containsKey(headAlias)) {
                joinTableBuff.addAll(fkMap.get(headAlias));
            }
        }

        joinTables = orderedJoinTables;
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
    public String toString() {
        return "NDataModel [name=" + name + "]";
    }

    public static String concatResourcePath(String descName) {
        return ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public ProjectInstance getProjectInstance() {
        return NProjectManager.getInstance(getConfig()).getProject(project);
    }

    public NDataModel copy() {
        return getCopyOf(this);
    }

    public String getProject() {
        return project;
    }

    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<NDataModel> otherModels,
            boolean isOnlineModel, String project) {
        this.project = project;

        // tweak the tables according to Computed Columns defined in model
        Map<String, TableDesc> tables = Maps.newHashMap();
        for (Map.Entry<String, TableDesc> entry : originalTables.entrySet()) {
            String s = entry.getKey();
            TableDesc tableDesc = entry.getValue();

            // null is possible when only involved table metadata is copied to remote executor
            if (tableDesc == null)
                continue;

            TableDesc extendedTableDesc = tableDesc
                    .appendColumns(ComputedColumnUtil.createComputedColumns(computedColumnDescs, tableDesc), true);
            tables.put(s, extendedTableDesc);
        }

        init(config, tables);

        initComputedColumns(otherModels);
        initMultilevelPartitionCols();
        this.effectiveCols = initAllNamedColumns(NamedColumn::isExist);
        this.effectiveDimensions = initAllNamedColumns(NamedColumn::isDimension);
        initAllMeasures();
        initFk2Pk();
        checkSingleIncrementingLoadingTable();
    }

    public void checkSingleIncrementingLoadingTable() {
        if (this.getJoinTables() == null) {
            return;
        }
        for (val table : this.getJoinTables()) {
            if (table.getTableRef() != null && table.getTableRef().getTableDesc().isFact())
                throw new IllegalStateException("Only one incrementing loading table can be setted in model!");
        }
    }

    private ImmutableBiMap<Integer, TblColRef> initAllNamedColumns(Predicate<NamedColumn> filter) {
        List<TblColRef> all = new ArrayList<>(allNamedColumns.size());
        ImmutableBiMap.Builder<Integer, TblColRef> mapBuilder = ImmutableBiMap.builder();
        for (NamedColumn d : allNamedColumns) {
            if (!d.isExist()) {
                continue;
            }
            TblColRef col = this.findColumn(d.aliasDotColumn);
            d.aliasDotColumn = col.getIdentity();
            all.add(col);

            if (filter.test(d)) {
                mapBuilder.put(d.id, col);
            }
        }

        val cols = mapBuilder.build();
        checkNoDup(cols);
        return cols;
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
        ImmutableBiMap.Builder<Integer, Measure> mapBuilder = ImmutableBiMap.builder();
        for (Measure m : allMeasures) {
            try {
                m.setName(m.getName().toUpperCase());

                if (!m.tomb) {
                    mapBuilder.put(m.id, m);
                    FunctionDesc func = m.getFunction();
                    func.init(this);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Cannot init measure " + m.getName() + ": " + e.getMessage(), e);
            }
        }

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
            if (m.getFunction().isCountConstant())
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

    public List<Measure> getAllMeasures() {
        return allMeasures;
    }

    /**
     * returns ID <==> TblColRef
     */
    public ImmutableBiMap<Integer, TblColRef> getEffectiveColsMap() {
        return effectiveCols;
    }

    public ImmutableBiMap<Integer, TblColRef> getEffectiveDimenionsMap() {
        return effectiveDimensions;
    }

    /**
     * returns ID <==> Measure
     */
    public ImmutableBiMap<Integer, Measure> getEffectiveMeasureMap() {
        return effectiveMeasures;
    }

    //TODO: !!! check the returned
    public @Nullable Integer getColId(TblColRef colRef) {
        return effectiveCols.inverse().get(colRef);
    }

    public TblColRef getColRef(Integer colId) {
        return effectiveCols.get(colId);
    }

    public boolean isExtendedColumn(TblColRef tblColRef) {
        return false; // TODO: enable derived
    }

    private void initMultilevelPartitionCols() {
        mpCols = Arrays.asList(new TblColRef[mpColStrs.size()]);
        if (CollectionUtils.isEmpty(mpColStrs))
            return;

        StringUtil.toUpperCaseArray(mpColStrs, mpColStrs);

        for (int i = 0; i < mpColStrs.size(); i++) {
            mpCols.set(i, findColumn(mpColStrs.get(i)));
            mpColStrs.set(i, mpCols.get(i).getIdentity());

            DataType type = mpCols.get(i).getType();
            if (!type.isNumberFamily() && !type.isStringFamily())
                throw new IllegalStateException(
                        "Multi-level partition column must be Number or String, but " + mpCols.get(i) + " is " + type);
        }

        checkMPColsBelongToModel(mpCols);
    }

    private void checkMPColsBelongToModel(List<TblColRef> tcr) {
        Set<TblColRef> refSet = effectiveCols.values();
        if (!refSet.containsAll(Sets.newHashSet(tcr))) {
            throw new IllegalStateException("Primary partition column should inside of this model.");
        }
    }

    private void initComputedColumns(List<NDataModel> otherModels) {
        Preconditions.checkNotNull(otherModels);

        // init
        for (ComputedColumnDesc newCC : this.computedColumnDescs) {
            newCC.init(this, getRootFactTable().getAlias());
        }

        if (!"true".equals(System.getProperty("needCheckCC"))) {
            return;
        }

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {
            Set<String> usedAliasSet = getUsedAliasSet(newCC.getExpression());

            if (!this.isSeekingCCAdvice() //if is seeking for advice, expr will be null
                    && !usedAliasSet.contains(newCC.getTableAlias())
                    && !newCC.getTableAlias().equals(getRootFactTable().getAlias())) {
                throw new BadModelException(
                        "A computed column should be defined on root fact table if its expression is not referring its hosting alias table, cc: "
                                + newCC.getFullName(),
                        BadModelException.CauseType.LOOKUP_CC_NOT_REFERENCING_ITSELF, null, null, newCC.getFullName());
            }
        }
        checkCCExprHealth();
        selfCCConflictCheck();
        crossCCConflictCheck(otherModels);
    }

    private void checkCCExprHealth() {
        for (ComputedColumnDesc ccDesc : computedColumnDescs) {
            Set<String> ccUsedCols = ComputedColumnUtil.getCCUsedColsWithModel(this, ccDesc);
            for (String tblCol : ccUsedCols) {
                String table = tblCol.substring(0, tblCol.lastIndexOf("."));
                String column = tblCol.substring(tblCol.lastIndexOf(".") + 1);
                TableRef tableRef = this.findFirstTable(table);
                TblColRef col = tableRef.getColumn(column);
                if (col == null) {
                    throw new IllegalArgumentException(
                            "Computed Column " + ccDesc.getColumnName() + " use incorrect column");
                }
            }
        }
    }

    private void selfCCConflictCheck() {
        int ccCount = this.computedColumnDescs.size();
        for (int i = 1; i < ccCount; i++) {
            for (int j = 0; j < i; j++) {
                ComputedColumnDesc a = this.computedColumnDescs.get(i);
                ComputedColumnDesc b = this.computedColumnDescs.get(j);
                // self check, two cc cannot define same cc column name, even if it's on different alias table
                if (StringUtils.equalsIgnoreCase(a.getColumnName(), b.getColumnName())) {
                    throw new BadModelException(
                            "In current model, at least two computed columns share the same column name: "
                                    + a.getColumnName() + ", please use different column name",
                            BadModelException.CauseType.SELF_CONFLICT, null, null, a.getFullName());
                }
                // self check, two cc cannot define same expression
                if (isLiteralSameCCExpr(a, b)) {
                    throw new BadModelException(
                            "In current model, computed column " + a.getFullName() + " share same expression as "
                                    + b.getFullName() + ", please remove one",
                            BadModelException.CauseType.SELF_CONFLICT, null, null, a.getFullName());
                }
            }
        }
    }

    // check duplication with other models:
    private void crossCCConflictCheck(List<NDataModel> otherModels) {

        List<Pair<ComputedColumnDesc, NDataModel>> existingCCs = Lists.newArrayList();
        for (NDataModel otherModel : otherModels) {
            if (!StringUtils.equals(otherModel.getName(), this.getName())) { // when update, self is already in otherModels
                for (ComputedColumnDesc cc : otherModel.getComputedColumnDescs()) {
                    existingCCs.add(Pair.newPair(cc, otherModel));
                }
            }
        }

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {
            for (Pair<ComputedColumnDesc, NDataModel> pair : existingCCs) {
                NDataModel existingModel = pair.getSecond();
                ComputedColumnDesc existingCC = pair.getFirst();
                singleCCConflictCheck(existingModel, existingCC, newCC);
            }
        }
    }

    private void singleCCConflictCheck(NDataModel existingModel, ComputedColumnDesc existingCC,
            ComputedColumnDesc newCC) {
        AliasMapping aliasMapping = getCCAliasMapping(existingModel, existingCC, newCC);
        boolean sameName = StringUtils.equalsIgnoreCase(existingCC.getColumnName(), newCC.getColumnName());
        boolean sameCCExpr = isSameCCExpr(existingCC, newCC, aliasMapping);

        if (sameName) {
            if (!isSameAliasTable(existingCC, newCC, aliasMapping)) {
                makeAdviseOnWrongPositionName(existingModel, existingCC, newCC, aliasMapping);
            }

            if (!sameCCExpr) {
                makeAdviseOnSameNameDiffExpr(existingModel, existingCC, newCC);
            }
        }

        if (sameCCExpr) {
            if (!isSameAliasTable(existingCC, newCC, aliasMapping)) {
                makeAdviseOnWrongPositionExpr(existingModel, existingCC, newCC, aliasMapping);
            }

            if (!sameName) {
                makeAdviseOnSameExprDiffName(existingModel, existingCC, newCC);
            }
        }
    }

    private void makeAdviseOnSameNameDiffExpr(NDataModel existingModel, ComputedColumnDesc existingCC,
            ComputedColumnDesc newCC) {
        JoinsGraph ccJoinsGraph = getCCExprRelatedSubgraph(existingCC, existingModel);
        AliasMapping aliasMapping = getAliasMappingFromJoinsGraph(ccJoinsGraph, this.getJoinsGraph());
        String advisedExpr = aliasMapping == null ? null
                : CalciteParser.replaceAliasInExpr(existingCC.getExpression(), aliasMapping.getAliasMapping());

        String msg = String.format(
                "Column name for computed column %s is already used in model %s, you should apply the same expression %s here, or use a different computed column name.",
                newCC.getFullName(), existingModel.getName(),
                advisedExpr != null ? "as \' " + advisedExpr + " \'" : "like \' " + existingCC.getExpression() + " \'");
        throw new BadModelException(msg, BadModelException.CauseType.SAME_NAME_DIFF_EXPR, advisedExpr,
                existingModel.getName(), newCC.getFullName());
    }

    private AliasMapping getCCAliasMapping(NDataModel existingModel, ComputedColumnDesc existingCC,
            ComputedColumnDesc newCC) {
        JoinsGraph newCCGraph = getCCExprRelatedSubgraph(newCC, this);
        JoinsGraph existCCGraph = getCCExprRelatedSubgraph(existingCC, existingModel);
        return getAliasMappingFromJoinsGraph(newCCGraph, existCCGraph);
    }

    private static AliasMapping getAliasMappingFromJoinsGraph(JoinsGraph fromGraph, JoinsGraph toMatchGraph) {
        AliasMapping adviceAliasMapping = null;

        Map<String, String> matches = fromGraph.matchAlias(toMatchGraph, true);
        if (matches != null && !matches.isEmpty()) {
            BiMap<String, String> biMap = HashBiMap.create();
            biMap.putAll(matches);
            adviceAliasMapping = new AliasMapping(biMap);
        }
        return adviceAliasMapping;
    }

    private void makeAdviseOnSameExprDiffName(NDataModel existingModel, ComputedColumnDesc existingCC,
            ComputedColumnDesc newCC) {
        String adviseName = existingCC.getColumnName();
        String msg = String.format(
                "Expression %s in computed column %s is already defined by computed column %s from model %s, you should use the same column name: ' %s ' .",
                newCC.getExpression(), newCC.getFullName(), existingCC.getFullName(), existingModel.getName(),
                existingCC.getColumnName());
        throw new BadModelException(msg, BadModelException.CauseType.SAME_EXPR_DIFF_NAME, adviseName,
                existingModel.getName(), newCC.getFullName());
    }

    private void makeAdviseOnWrongPositionExpr(NDataModel existingModel, ComputedColumnDesc existingCC,
            ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
        String advice = positionAliasMapping == null ? null
                : positionAliasMapping.getAliasMapping().get(existingCC.getTableAlias());

        String msg = null;

        if (advice != null) {
            msg = String.format(
                    "Computed column %s's expression is already defined in model %s, to reuse it you have to define it on alias table: %s",
                    newCC.getColumnName(), existingModel.getName(), advice);
        } else {
            msg = String.format(
                    "Computed column %s's expression is already defined in model %s, no suggestion could be provided to reuse it",
                    newCC.getColumnName(), existingModel.getName());
        }

        throw new BadModelException(msg, BadModelException.CauseType.WRONG_POSITION_DUE_TO_EXPR, advice,
                existingModel.getName(), newCC.getFullName());
    }

    private void makeAdviseOnWrongPositionName(NDataModel existingModel, ComputedColumnDesc existingCC,
            ComputedColumnDesc newCC, AliasMapping positionAliasMapping) {
        String advice = positionAliasMapping == null ? null
                : positionAliasMapping.getAliasMapping().get(existingCC.getTableAlias());

        String msg = null;

        if (advice != null) {
            msg = String.format(
                    "Computed column %s is already defined in model %s, to reuse it you have to define it on alias table: %s",
                    newCC.getColumnName(), existingModel.getName(), advice);
        } else {
            msg = String.format(
                    "Computed column %s is already defined in model %s, no suggestion could be provided to reuse it",
                    newCC.getColumnName(), existingModel.getName());
        }

        throw new BadModelException(msg, BadModelException.CauseType.WRONG_POSITION_DUE_TO_NAME, advice,
                existingModel.getName(), newCC.getFullName());
    }

    private boolean isSameCCExpr(ComputedColumnDesc existingCC, ComputedColumnDesc newCC, AliasMapping aliasMapping) {
        if (existingCC.getExpression() == null) {
            return newCC.getExpression() == null;
        } else if (newCC.getExpression() == null) {
            return false;
        }

        return ExpressionComparator.isNodeEqual(CalciteParser.getExpNode(newCC.getExpression()),
                CalciteParser.getExpNode(existingCC.getExpression()), aliasMapping, AliasDeduce.NO_OP);
    }

    private boolean isSameAliasTable(ComputedColumnDesc existingCC, ComputedColumnDesc newCC,
            AliasMapping adviceAliasMapping) {
        if (adviceAliasMapping == null) {
            return false;
        }
        String existingAlias = existingCC.getTableAlias();
        String newAlias = newCC.getTableAlias();
        return StringUtils.equals(newAlias, adviceAliasMapping.getAliasMapping().get(existingAlias));
    }

    private boolean isLiteralSameCCExpr(ComputedColumnDesc existingCC, ComputedColumnDesc newCC) {
        String definition0 = existingCC.getExpression();
        String definition1 = newCC.getExpression();

        if (definition0 == null) {
            return definition1 == null;
        } else if (definition1 == null) {
            return false;
        }

        definition0 = definition0.replaceAll("\\s*", "");
        definition1 = definition1.replaceAll("\\s*", "");
        return definition0.equalsIgnoreCase(definition1);
    }

    // model X contains table f,a,b,c, and model Y contains table f,a,b,d
    // if two cc involve table a,b, they might still be treated equal regardless of the model difference on c,d
    private static JoinsGraph getCCExprRelatedSubgraph(ComputedColumnDesc cc, NDataModel model) {
        Set<String> aliasSets = getUsedAliasSet(cc.getExpression());
        if (cc.getTableAlias() != null) {
            aliasSets.add(cc.getTableAlias());
        }
        return model.getJoinsGraph().getSubgraphByAlias(aliasSets);
    }

    private static Set<String> getUsedAliasSet(String expr) {
        if (expr == null) {
            return Sets.newHashSet();
        }
        SqlNode sqlNode = CalciteParser.getExpNode(expr);

        final Set<String> s = Sets.newHashSet();
        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                Preconditions.checkState(id.names.size() == 2);
                s.add(id.names.get(0));
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        return s;
    }

    public ComputedColumnDesc findCCByCCColumnName(final String columnName) {
        return this.computedColumnDescs.stream().filter(input -> {
            Preconditions.checkNotNull(input);
            return columnName.equals(input.getColumnName());
        }).findFirst().orElse(null);
    }

    public String getAlias() {
        if (StringUtils.isEmpty(this.alias)) {
            return this.name;
        }
        return this.alias;
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
        return mpColStrs.size() > 0;
    }

    public List<String> getMutiLevelPartitionColStrs() {
        return mpColStrs;
    }

    void setMutiLevelPartitionColStrs(List<String> colStrs) {
        this.mpColStrs = colStrs;
    }

    public List<TblColRef> getMutiLevelPartitionCols() {
        return mpCols;
    }

    public List<String> getMpColStrs() {
        return mpColStrs;
    }

    void setMpColStrs(List<String> mpColStrs) {
        this.mpColStrs = mpColStrs;
    }

    public boolean isSeekingCCAdvice() {
        return isSeekingCCAdvice;
    }

    public void setSeekingCCAdvice(boolean seekingCCAdvice) {
        isSeekingCCAdvice = seekingCCAdvice;
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

    public String getNameByColumnId(int id) {
        Preconditions.checkArgument(allNamedColumns != null);
        for (NamedColumn col : allNamedColumns) {
            if (col.id == id)
                return col.name;
        }
        return null;
    }

    public static NDataModel getCopyOf(NDataModel orig) {
        return (NDataModel) SerializationUtils.clone(orig);
    }

    @Override
    public String getResourcePath() {
        return new StringBuilder().append("/").append(project).append(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                .append("/").append(getName()).append(MetadataConstants.FILE_SURFIX).toString();
    }

}
