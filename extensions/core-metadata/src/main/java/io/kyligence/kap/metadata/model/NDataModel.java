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

package io.kyligence.kap.metadata.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;

@SuppressWarnings("serial")
public class NDataModel extends DataModelDesc {
    public static final int MEASURE_ID_BASE = 1000;

    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    public static class NamedColumn implements Serializable, IKeep {
        @JsonProperty("id")
        public int id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("column")
        public String aliasDotColumn;
    }

    public static class Measure extends MeasureDesc implements IKeep {
        @JsonProperty("id")
        public int id;
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

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NDataModel.class);

    private static final String DEL_MARK = "[DELETED]";

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
    private Map<TableRef, BitSet> effectiveDerivedCols;
    private TblColRef[] mpCols;

    // don't use unless you're sure, for jackson only
    public NDataModel() {
        super();
    }

    @Override
    public String getProject() {
        return project;
    }

    @Override
    public void setProject(String project) {
        this.project = project;
    }

    @Override
    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<DataModelDesc> otherModels,
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

        super.init(config, tables, otherModels, isOnlineModel);

        initComputedColumns(otherModels);
        initMultilevelPartitionCols();
        initAllNamedColumns();
        initAllMeasures();
        initDerivedCols();
    }

    private void initDerivedCols() {
        Map<TableRef, BitSet> effectiveDerivedCols = Maps.newLinkedHashMap();
        for (Map.Entry<Integer, TblColRef> colEntry : effectiveCols.entrySet()) {
            TableRef tblRef = colEntry.getValue().getTableRef();
            if (isLookupTable(tblRef)) {
                BitSet cols = effectiveDerivedCols.get(tblRef);
                if (cols == null) {
                    cols = new BitSet();
                    effectiveDerivedCols.put(tblRef, cols);
                }
                cols.set(colEntry.getKey());
            }
        }
        this.effectiveDerivedCols = effectiveDerivedCols;
    }

    private void initAllNamedColumns() {
        List<TblColRef> all = new ArrayList<>(allNamedColumns.size());
        ImmutableBiMap.Builder<Integer, TblColRef> mapBuilder = ImmutableBiMap.builder();
        for (NamedColumn d : allNamedColumns) {
            TblColRef col = this.findColumn(d.aliasDotColumn);
            d.aliasDotColumn = col.getIdentity();
            all.add(col);

            if (!d.name.startsWith(DEL_MARK)) {
                mapBuilder.put(d.id, col);
            }
        }

        this.allCols = all;
        this.effectiveCols = mapBuilder.build();
        checkNoDup(effectiveCols);
    }

    private <T> void checkNoDup(ImmutableBiMap<Integer, T> idMap) {
        Map<T, Integer> reverseMap = new HashMap<>();
        for (Entry<Integer, T> e : idMap.entrySet()) {
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

            if (!m.getName().startsWith(DEL_MARK)) {
                mapBuilder.put(m.id, m);
            }
        }

        this.allMeasures = all;
        this.effectiveMeasures = mapBuilder.build();
        checkNoDupAndEffective(effectiveMeasures);
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

    /** returns TableRef ===> DimensionIds */
    public Map<TableRef, BitSet> getEffectiveDerivedColsMap() {
        return effectiveDerivedCols;
    }

    /** returns ID <==> TblColRef */
    public ImmutableBiMap<Integer, TblColRef> getEffectiveColsMap() {
        return effectiveCols;
    }

    /** returns ID <==> Measure */
    public ImmutableBiMap<Integer, Measure> getEffectiveMeasureMap() {
        return effectiveMeasures;
    }

    public DeriveInfo getDerivedHost(TblColRef col) {
        TableRef pkTblRef = col.getTableRef();
        Preconditions.checkState(effectiveDerivedCols.containsKey(pkTblRef), "Column %s cannot be derived.",
                col.getIdentity());
        JoinDesc joinDesc = getJoinByPKSide(pkTblRef);
        return new DeriveInfo(DeriveInfo.DeriveType.LOOKUP, joinDesc, joinDesc.getForeignKeyColumns(), false);
    }

    public int getColId(TblColRef colRef) {
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
        if (refSet.containsAll(Sets.newHashSet(tcr)) == false) {
            throw new IllegalStateException("Primary partition column should inside of this model.");
        }
    }

    private void initComputedColumns(List<DataModelDesc> otherModels) {
        Preconditions.checkNotNull(otherModels);

        List<Pair<ComputedColumnDesc, NDataModel>> existingCCs = Lists.newArrayList();

        for (DataModelDesc dataModelDesc : otherModels) {
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
                DataModelDesc dataModelDesc = pair.getSecond();
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
        NDataModel copy = (NDataModel) DataModelDesc.copy(orig, new NDataModel());
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