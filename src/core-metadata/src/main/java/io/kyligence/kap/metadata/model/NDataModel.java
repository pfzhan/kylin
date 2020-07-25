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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
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
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.scheduler.SchedulerEventNotifier;
import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.val;

@Data
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class NDataModel extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(NDataModel.class);
    public static final int MEASURE_ID_BASE = 100000;

    public enum TableKind implements Serializable {
        FACT, LOOKUP
    }

    public enum RealizationCapacity implements Serializable {
        SMALL, MEDIUM, LARGE
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ModelBrokenEvent extends SchedulerEventNotifier {
        public ModelBrokenEvent(String project, String subject) {
            this.project = project;
            this.subject = subject;
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ModelRepairEvent extends SchedulerEventNotifier {
        public ModelRepairEvent(String project, String subject) {
            this.project = project;
            this.subject = subject;
        }
    }

    @VisibleForTesting
    private KylinConfig config;

    @EqualsAndHashCode.Include
    @JsonProperty("alias")
    private String alias;

    @EqualsAndHashCode.Include
    @JsonProperty("owner")
    private String owner;

    @JsonProperty("config_last_modifier")
    private String configLastModifier;

    @JsonProperty("config_last_modified")
    private long configLastModified;

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

    @JsonProperty("segment_config")
    private SegmentConfig segmentConfig = new SegmentConfig();

    @JsonProperty("data_check_desc")
    private DataCheckDesc dataCheckDesc;

    @JsonProperty("semantic_version")
    private int semanticVersion;

    @JsonProperty("storage_type")
    private int storageType;

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

    @JsonProperty("broken_reason")
    @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = BrokenReasonFilter.class)
    private BrokenReason brokenReason = BrokenReason.NULL;

    @JsonProperty("handled_after_broken")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean handledAfterBroken = false;

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

    public enum BrokenReason {
        SCHEMA, NULL, EVENT
    }

    @Data
    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    @EqualsAndHashCode
    @ToString
    public static class NamedColumn implements Serializable, IKeep {
        @JsonProperty("id")
        protected int id;

        @JsonProperty("name")
        protected String name;

        @JsonProperty("column")
        protected String aliasDotColumn;

        // logical delete symbol
        @JsonProperty("status")
        @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = ColumnStatusFilter.class)
        protected ColumnStatus status = ColumnStatus.EXIST;

        public boolean isExist() {
            return status != ColumnStatus.TOMB;
        }

        public boolean isDimension() {
            return status == ColumnStatus.DIMENSION;
        }

        public void changeTableAlias(String oldAlias, String newAlias) {
            String table = aliasDotColumn.split("\\.")[0];
            String column = aliasDotColumn.split("\\.")[1];
            if (table.equalsIgnoreCase(oldAlias)) {
                aliasDotColumn = newAlias + "." + column;
            }
        }
    }

    @Data
    @EqualsAndHashCode
    public static class Measure extends MeasureDesc implements IKeep {
        @JsonProperty("id")
        private int id;
        // logical delete symbol
        @Getter
        @Setter
        @JsonProperty("tomb")
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        private boolean tomb = false;

        public void changeTableAlias(String oldAlias, String newAlias) {
            for (val parameter : getFunction().getParameters()) {
                String table = parameter.getValue().split("\\.")[0];
                if (oldAlias.equalsIgnoreCase(table)) {
                    String column = parameter.getValue().split("\\.")[1];
                    parameter.setValue(newAlias + "." + column);
                }
            }
        }

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
        this.createTime = other.createTime;
        this.lastModified = other.lastModified;
        this.version = other.version;
        this.alias = other.alias;
        this.owner = other.owner;
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
        this.segmentConfig = other.segmentConfig;
        this.dataCheckDesc = other.dataCheckDesc;
        this.canvas = other.canvas;
        this.brokenReason = other.brokenReason;
        this.configLastModifier = other.configLastModifier;
        this.configLastModified = other.configLastModified;
        this.semanticVersion = other.semanticVersion;
    }

    public KylinConfig getConfig() {
        return config;
    }

    @Override
    public String resourceName() {
        return uuid;
    }

    public ManagementType getManagementType() {
        return managementType;
    }

    public void setManagementType(ManagementType managementType) {
        this.managementType = managementType;
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
        if (table == null)
            return false;

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

    public TblColRef findColumnByAlias(String column) {
        TblColRef result = null;

        column = column.toUpperCase();
        int cut = column.lastIndexOf('.');
        String table = column.substring(0, cut);
        String col = column.substring(cut + 1);

        for (TableRef tableRef : allTableRefs) {
            if (tableRef.getAlias().equals(table)) {
                result = tableRef.getColumn(col);
            }
            if (result != null)
                break;
        }

        return result;
    }

    // find by unique name, that must uniquely identifies a table in the model
    public TableRef findTable(String table) throws IllegalArgumentException {
        TableRef result = tableNameMap.get(table.toUpperCase());
        if (result == null) {
            int endOfDatabaseName = table.indexOf(".");
            if (endOfDatabaseName > -1) {
                result = tableNameMap.get(table.substring(endOfDatabaseName + 1));
            }
            if (result == null) {
                throw new IllegalArgumentException("Table not found by " + table);
            }
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
        throw new IllegalArgumentException("Table not found by " + tableIdentity + " in model " + uuid);
    }

    public void initJoinDesc(KylinConfig config, Map<String, TableDesc> tables) {
        this.config = config;

        initJoinTablesForUpgrade();
        initTableAlias(tables);
        initJoinColumns();
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
            this.alias = this.uuid;
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
            join.setPrimaryTableRef(dimTable);

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
            if (join.getForeignTable() != null && findTable(join.getForeignTable()) != null) {
                join.setForeignTableRef(findTable(join.getForeignTable()));
            }

            // non equi joins
            initNonEquiCondition(join.getNonEquiJoinCondition());

            join.sortByFK();

            // Validate join in dimension
            if (pkCols.length != fkCols.length) {
                throw new IllegalStateException("Primary keys(" + dimTable + ")" + Arrays.toString(pks)
                        + " are not consistent with Foreign keys(" + join.getFKSide().getTableIdentity() + ") "
                        + Arrays.toString(fks));
            }
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    logger.warn("PK " + dimTable + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype()
                            + " are not consistent with FK " + join.getFKSide().getTableIdentity() + "."
                            + fkCols[i].getName() + "." + fkCols[i].getDatatype());
                }
            }
        }
    }

    private void initNonEquiCondition(NonEquiJoinCondition cond) {
        if (cond == null) {
            return;
        }

        if (cond.getType() == NonEquiJoinConditionType.COLUMN) {
            cond.setColRef(findColumn(cond.getValue()));
        }
        if (cond.getOperands().length > 0) {
            for (NonEquiJoinCondition childInput : cond.getOperands()) {
                initNonEquiCondition(childInput);
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
        return "NDataModel [" + getAlias() + "]";
    }

    public ProjectInstance getProjectInstance() {
        return NProjectManager.getInstance(getConfig()).getProject(project);
    }

    public String getProject() {
        return project;
    }

    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<NDataModel> otherModels,
            String project) {
        init(config, originalTables, otherModels, project, false);
    }

    public Map<String, TableDesc> getExtendedTables(Map<String, TableDesc> originalTables) {
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
        return tables;
    }

    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<NDataModel> otherModels,
            String project, boolean rename) {
        this.project = project;

        Map<String, TableDesc> tables = getExtendedTables(originalTables);

        init(config, tables);

        initComputedColumns(otherModels);
        initMultilevelPartitionCols();
        this.effectiveCols = initAllNamedColumns(NamedColumn::isExist);
        this.effectiveDimensions = initAllNamedColumns(NamedColumn::isDimension);
        initAllMeasures();
        initFk2Pk();
        checkSingleIncrementingLoadingTable();
        setDependencies(calcDependencies());

        ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(getProject());
        if (Objects.nonNull(projectInstance)
                && projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN) {
            if (isIncrementBuildOnExpertMode()) {
                val incrementLookupTables = otherModels.stream().filter(m -> !m.getId().equals(getId()))
                        .flatMap(model -> model.getJoinTables().stream()
                                .filter(joinTableDesc -> joinTableDesc.getKind() == TableKind.LOOKUP)
                                .map(JoinTableDesc::getTable))
                        .collect(Collectors.toSet());

                if (incrementLookupTables.contains(getRootFactTableName())) {
                    throw new LookupTableException(MsgPicker.getMsg().getFACT_TABLE_USED_IN_OTHER_MODEL());
                }
            }

            val incrementRootFactTables = otherModels.stream().filter(m -> !m.getId().equals(getId()))
                    .filter(NDataModel::isIncrementBuildOnExpertMode).map(NDataModel::getRootFactTable)
                    .map(TableRef::getTableIdentity).collect(Collectors.toSet());
            incrementRootFactTables.add(getRootFactTableName());
            val lookups = getJoinTables().stream().filter(joinTableDesc -> joinTableDesc.getKind() == TableKind.LOOKUP)
                    .map(JoinTableDesc::getTable).collect(Collectors.toSet());

            if (!Collections.disjoint(incrementRootFactTables, lookups)) {
                throw new LookupTableException(MsgPicker.getMsg().getDIMENSION_TABLE_USED_IN_OTHER_MODEL());
            }
        }
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {

        Set<String> dependTables = Sets.newHashSet();
        dependTables.add(getRootFactTableName());
        dependTables.addAll(this.getJoinTables().stream().map(JoinTableDesc::getTable).collect(Collectors.toList()));

        return dependTables.stream().filter(Objects::nonNull).map(t -> {

            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(t);
            return tableDesc != null ? tableDesc
                    : new MissingRootPersistentEntity(TableDesc.concatResourcePath(t, project));

        }).collect(Collectors.toList());
    }

    public boolean isIncrementBuildOnExpertMode() {
        if (NProjectManager.getInstance(this.config).getProject(getProject())
                .getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN) {
            return getPartitionDesc() != null && !StringUtils.isEmpty(partitionDesc.getPartitionDateColumn())
                    && !StringUtils.isEmpty(partitionDesc.getPartitionDateFormat());
        }
        return false;
    }

    public void checkSingleIncrementingLoadingTable() {
        if (this.getJoinTables() == null) {
            return;
        }
        for (val table : this.getJoinTables()) {
            if (table.getTableRef() != null && table.getTableRef().getTableDesc().isIncrementLoading())
                throw new IllegalStateException("Only one incremental loading table can be set in model!");
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
                throw new IllegalStateException(String.format("Illegal model '%d', %s has duplicated ID: %s and %d", id,
                        value, reverseMap.get(value), id));
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
            throw new IllegalStateException(
                    String.format("Illegal model '%s', should have one and only one COUNT() measure but there are %d",
                            uuid, countNum));

        // check all measure columns are effective
        for (MeasureDesc m : effectiveMeasures.values()) {
            List<TblColRef> mCols = m.getFunction().getColRefs();
            if (effectiveCols.values().containsAll(mCols) == false) {
                List<TblColRef> notEffective = new ArrayList<>(mCols);
                notEffective.removeAll(effectiveCols.values());
                throw new IllegalStateException(
                        String.format("Illegal model '%s', some columns referenced in %s is not on model: %s", uuid, m,
                                notEffective));
            }
        }
    }

    //TODO: !!! check the returned
    public @Nullable Integer getColId(TblColRef colRef) {
        return effectiveCols.inverse().get(colRef);
    }

    public TblColRef getColRef(Integer colId) {
        return effectiveCols.get(colId);
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

    public void initComputedColumns(List<NDataModel> otherModels) {
        Preconditions.checkNotNull(otherModels);

        // init
        for (ComputedColumnDesc newCC : this.computedColumnDescs) {
            newCC.init(this, getRootFactTable().getAlias());
        }

        if (!"true".equals(System.getProperty("needCheckCC"))) {
            return;
        }

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {
            Set<String> usedAliasSet = ComputedColumnUtil.getUsedAliasSet(newCC.getExpression());

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
                            "Computed Column " + ccDesc.getColumnName() + " use nonexistent column(s): " + tblCol);
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
                if (ComputedColumnUtil.isLiteralSameCCExpr(a, b)) {
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
            if (!StringUtils.equals(otherModel.getUuid(), this.getUuid())) { // when update, self is already in otherModels
                for (ComputedColumnDesc cc : otherModel.getComputedColumnDescs()) {
                    existingCCs.add(Pair.newPair(cc, otherModel));
                }
            }
        }

        val handler = new ComputedColumnUtil.DefaultCCConflictHandler();

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {
            for (Pair<ComputedColumnDesc, NDataModel> pair : existingCCs) {
                NDataModel existingModel = pair.getSecond();
                ComputedColumnDesc existingCC = pair.getFirst();
                ComputedColumnUtil.singleCCConflictCheck(existingModel, this, existingCC, newCC, handler);
            }
        }
    }

    public ComputedColumnDesc findCCByCCColumnName(final String columnName) {
        return this.computedColumnDescs.stream().filter(input -> {
            Preconditions.checkNotNull(input);
            return columnName.equals(input.getColumnName());
        }).findFirst().orElse(null);
    }

    public String getAlias() {
        if (StringUtils.isEmpty(this.alias)) {
            return this.uuid;
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

    public static boolean isMeasureId(int id) {
        return id >= MEASURE_ID_BASE;
    }

    public void setSeekingCCAdvice(boolean seekingCCAdvice) {
        isSeekingCCAdvice = seekingCCAdvice;
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

    public ImmutableMultimap<TblColRef, TblColRef> getFk2Pk() {
        return fk2Pk;
    }

    public Map<Integer, NamedColumn> getEffectiveNamedColumns() {
        return allNamedColumns.stream().filter(NamedColumn::isExist)
                .collect(Collectors.toMap(NamedColumn::getId, Function.identity()));
    }

    public Map<String, Integer> getDimensionNameIdMap() {
        Preconditions.checkArgument(Objects.nonNull(allNamedColumns));
        return allNamedColumns.stream().filter(NamedColumn::isDimension)
                .collect(Collectors.toMap(NamedColumn::getAliasDotColumn, NamedColumn::getId));
    }

    public int getColumnIdByColumnName(String aliasDotName) {
        Preconditions.checkArgument(Objects.nonNull(allNamedColumns));
        return allNamedColumns.stream()
                .filter(col -> col.aliasDotColumn.equalsIgnoreCase(aliasDotName) && col.isExist())
                .map(NamedColumn::getId).findAny().orElse(-1);
    }

    public String getColumnNameByColumnId(int id) {
        Preconditions.checkArgument(Objects.nonNull(allNamedColumns));
        return allNamedColumns.stream().filter(col -> Objects.equals(col.getId(), id) && col.isExist())
                .map(NamedColumn::getAliasDotColumn).findAny().orElse(null);
    }

    public String getNameByColumnId(int id) {
        Preconditions.checkArgument(Objects.nonNull(allNamedColumns));
        return allNamedColumns.stream().filter(col -> Objects.equals(col.getId(), id) && col.isExist())
                .map(NamedColumn::getName).findAny().orElse(null);
    }

    public String getMeasureNameByMeasureId(int id) {
        Preconditions.checkArgument(Objects.nonNull(allMeasures));
        return allMeasures.stream().filter(mea -> Objects.equals(mea.getId(), id) && !mea.isTomb())
                .map(Measure::getName).findAny().orElse(null);
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)
                .append("/").append(name).append(MetadataConstants.FILE_SURFIX).toString();

    }

    public Collection<NamedColumn> getAllSelectedColumns() {
        Set<NamedColumn> selectedColumns = new HashSet<>();
        for (NamedColumn namedColumn : allNamedColumns) {
            if (namedColumn.getStatus() == ColumnStatus.DIMENSION) {
                selectedColumns.add(namedColumn);
            }
        }

        for (Measure measure : allMeasures) {
            if (measure.tomb) {
                continue;
            }

            for (TblColRef tblColRef: measure.getFunction().getColRefs()) {
                if (tblColRef != null) {
                    for (NamedColumn namedColumn : allNamedColumns) {
                        if (namedColumn.getAliasDotColumn().equalsIgnoreCase(tblColRef.getAliasDotName())) {
                            selectedColumns.add(namedColumn);
                        }
                    }
                }
            }
        }

        List<NamedColumn> result = new ArrayList<>(selectedColumns);
        result.sort(Comparator.comparingInt(NamedColumn::getId));
        return result;
    }

}
