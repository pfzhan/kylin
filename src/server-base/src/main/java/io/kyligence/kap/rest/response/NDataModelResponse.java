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

package io.kyligence.kap.rest.response;

import static io.kyligence.kap.metadata.model.NDataModel.ColumnStatus.DIMENSION;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import org.apache.commons.lang3.builder.HashCodeExclude;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.NDataModelAclParams;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.util.ModelUtils;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
public class NDataModelResponse extends NDataModel {

    @JsonProperty("status")
    private ModelStatusToDisplayEnum status;

    @JsonProperty("last_build_end")
    private String lastBuildEnd;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("source")
    private long source;

    @JsonProperty("model_type")
    private ModelType modelType;

    @JsonProperty("expansion_rate")
    private String expansionrate;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("root_fact_table_deleted")
    private boolean rootFactTableDeleted = false;

    @JsonProperty("segments")
    private List<NDataSegmentResponse> segments = new ArrayList<>();

    @JsonProperty("available_indexes_count")
    private long availableIndexesCount;

    @JsonProperty("empty_indexes_count")
    private long emptyIndexesCount;

    @JsonProperty("segment_holes")
    private List<SegmentRange> segmentHoles;

    @JsonProperty("inconsistent_segment_count")
    private long inconsistentSegmentCount;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    @JsonProperty("forbidden_online")
    private boolean forbiddenOnline = false;

    @JsonProperty("join_tables")
    private List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs;

    @JsonProperty("last_build_time")
    private long lastBuildTime;

    @JsonProperty("has_base_table_index")
    private boolean hasBaseTableIndex;

    @JsonProperty("has_base_agg_index")
    private boolean hasBaseAggIndex;

    @JsonProperty("second_storage_size")
    private long secondStorageSize;

    @JsonProperty("second_storage_nodes")
    private List<SecondStorageNode> secondStorageNodes;

    @JsonProperty("second_storage_enabled")
    private boolean secondStorageEnabled;

    private long lastModify;

    @JsonIgnore
    private List<SimplifiedNamedColumn> simplifiedDims;

    @Getter(lazy = true)
    @JsonIgnore
    private final NDataModel lazyModel = originModel();

    public NDataModelResponse() {
        super();
    }

    public NDataModelResponse(NDataModel dataModel) {
        super(dataModel);
        this.setConfig(dataModel.getConfig());
        this.setProject(dataModel.getProject());
        this.setMvcc(dataModel.getMvcc());
        this.setModelType(dataModel.getModelType());
        this.lastModify = lastModified;
        this.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(dataModel.getJoinTables()));
    }

    @JsonProperty("simplified_dimensions")
    public List<SimplifiedNamedColumn> getNamedColumns() {
        if (simplifiedDims != null) {
            return simplifiedDims;
        }
        fillDimensions(true);
        return simplifiedDims;
    }

    public void enrichDerivedDimension() {
        fillDimensions(false);
    }

    private void fillDimensions(boolean onlyNormalDim) {
        NTableMetadataManager tableMetadata = null;
        if (!isBroken()) {
            tableMetadata = NTableMetadataManager.getInstance(getConfig(), this.getProject());
        }

        Set<String> excludedTables = loadExcludedTables();
        List<JoinTableDesc> joinTables = getLazyModel().getJoinTables();
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, joinTables, getLazyModel());
        List<SimplifiedNamedColumn> dimList = Lists.newArrayList();
        for (NamedColumn col : getAllNamedColumns()) {
            if (col.isDimension()) {
                dimList.add(transColumnToDim(checker, col, tableMetadata));
            }
        }
        if (!onlyNormalDim) {
            List<NamedColumn> allNameColCopy = Lists.newArrayList();
            for (NamedColumn col : getAllNamedColumns()) {
                allNameColCopy.add(NamedColumn.copy(col));
            }

            Map<String, NamedColumn> columnMap = allNameColCopy.stream().filter(NamedColumn::isExist)
                    .collect(Collectors.toMap(NamedColumn::getAliasDotColumn, Function.identity()));
            for (JoinTableDesc joinTable : getJoinTables()) {
                if (!joinTable.isFlattenable() && isFkAllDim(joinTable.getJoin().getForeignKey(), columnMap)) {
                    for (TblColRef col : joinTable.getTableRef().getColumns()) {
                        NamedColumn namedColumn = columnMap.get(col.getAliasDotName());
                        if (!namedColumn.isDimension()) {
                            dimList.add(transColumnToDim(checker, namedColumn, tableMetadata));
                            namedColumn.setStatus(DIMENSION);
                        }
                    }
                }
            }
            setAllNamedColumns(allNameColCopy);
        }

        simplifiedDims = dimList;
    }

    private boolean isFkAllDim(String[] foreignKeys, Map<String, NamedColumn> columnMap) {
        if (foreignKeys == null) {
            return false;
        }
        for (String fkCol : foreignKeys) {
            if (!columnMap.get(fkCol).isDimension()) {
                return false;
            }
        }
        return true;
    }

    private NDataModel originModel() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataModelDesc(this.getUuid());
    }

    public SimplifiedNamedColumn transColumnToDim(ExcludedLookupChecker checker, NamedColumn col,
            NTableMetadataManager tableMetadata) {
        SimplifiedNamedColumn simplifiedDimension = new SimplifiedNamedColumn(col);
        simplifiedDimension.setStatus(DIMENSION);
        TblColRef colRef = findColumnByAlias(simplifiedDimension.getAliasDotColumn());
        if (colRef == null || tableMetadata == null) {
            return simplifiedDimension;
        }
        if (checker.isColRefDependsLookupTable(colRef)) {
            simplifiedDimension.setDependLookupTable(true);
        }
        TableExtDesc tableExt = tableMetadata.getTableExtIfExists(colRef.getTableRef().getTableDesc());
        if (tableExt != null) {
            TableExtDesc.ColumnStats columnStats = tableExt.getColumnStatsByName(colRef.getName());
            if (colRef.getColumnDesc().getComment() != null) {
                simplifiedDimension.setComment(colRef.getColumnDesc().getComment());
            }
            if (colRef.getColumnDesc().getType() != null) {
                simplifiedDimension.setType(colRef.getColumnDesc().getType().toString());
            }
            if (columnStats != null) {
                simplifiedDimension.setCardinality(columnStats.getCardinality());
                simplifiedDimension.setMaxValue(columnStats.getMaxValue());
                simplifiedDimension.setMinValue(columnStats.getMinValue());
                simplifiedDimension.setMaxLengthValue(columnStats.getMaxLengthValue());
                simplifiedDimension.setMinLengthValue(columnStats.getMinLengthValue());

                ArrayList<String> simple = Lists.newArrayList();
                tableExt.getSampleRows()
                        .forEach(row -> simple.add(row[tableExt.getAllColumnStats().indexOf(columnStats)]));
                simplifiedDimension.setSimple(simple);
            }
        }
        return simplifiedDimension;
    }

    @JsonProperty("all_measures")
    public List<Measure> getMeasures() {
        return getAllMeasures().stream().filter(m -> !m.isTomb()).collect(Collectors.toList());
    }

    @JsonProperty("model_broken")
    public boolean isModelBroken() {
        return this.isBroken();
    }

    @JsonProperty("simplified_tables")
    public List<SimplifiedTableResponse> getSimpleTables() {
        List<SimplifiedTableResponse> simpleTables = new ArrayList<>();
        for (TableRef tableRef : getAllTables()) {
            SimplifiedTableResponse simpleTable = new SimplifiedTableResponse();
            simpleTable.setTable(tableRef.getTableIdentity());
            List<SimplifiedColumnResponse> columns = getSimplifiedColumns(tableRef);
            simpleTable.setColumns(columns);
            simpleTables.add(simpleTable);
        }
        return simpleTables;
    }

    @JsonProperty("simplified_measures")
    public List<SimplifiedMeasure> getSimplifiedMeasures() {
        List<NDataModel.Measure> measures = getAllMeasures();
        List<SimplifiedMeasure> measureResponses = new ArrayList<>();
        for (NDataModel.Measure measure : measures) {
            if (measure.isTomb()) {
                continue;
            }
            measureResponses.add(SimplifiedMeasure.fromMeasure(measure));
        }
        return measureResponses;
    }

    private List<SimplifiedColumnResponse> getSimplifiedColumns(TableRef tableRef) {
        List<SimplifiedColumnResponse> columns = new ArrayList<>();
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getConfig(), getProject());
        for (ColumnDesc columnDesc : tableRef.getTableDesc().getColumns()) {
            TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableRef.getTableDesc());
            SimplifiedColumnResponse simplifiedColumnResponse = new SimplifiedColumnResponse();
            simplifiedColumnResponse.setName(columnDesc.getName());
            simplifiedColumnResponse.setComment(columnDesc.getComment());
            simplifiedColumnResponse.setDataType(columnDesc.getDatatype());
            simplifiedColumnResponse.setComputedColumn(columnDesc.isComputedColumn());
            // get column cardinality
            final TableExtDesc.ColumnStats columnStats = tableExtDesc.getColumnStatsByName(columnDesc.getName());
            if (columnStats != null) {
                simplifiedColumnResponse.setCardinality(columnStats.getCardinality());
            }

            columns.add(simplifiedColumnResponse);
        }
        return columns;
    }

    @Data
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @EqualsAndHashCode
    @ToString
    public static class SimplifiedNamedColumn extends NamedColumn implements Serializable, IKeep {

        public SimplifiedNamedColumn(NamedColumn namedColumn) {
            this.id = namedColumn.getId();
            this.aliasDotColumn = namedColumn.getAliasDotColumn();
            this.status = namedColumn.getStatus();
            this.name = namedColumn.getName();
        }

        @HashCodeExclude
        @JsonProperty("depend_lookup_table")
        private boolean dependLookupTable;

        @JsonProperty("cardinality")
        private Long cardinality;

        @JsonProperty("min_value")
        private String minValue;

        @JsonProperty("max_value")
        private String maxValue;

        @JsonProperty("max_length_value")
        private String maxLengthValue;

        @JsonProperty("min_length_value")
        private String minLengthValue;

        @JsonProperty("null_count")
        private Long nullCount;

        @JsonProperty("comment")
        private String comment;

        @JsonProperty("type")
        private String type;

        @JsonProperty("simple")
        private ArrayList<String> simple;
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelOldParams oldParams;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;

    @JsonGetter("selected_columns")
    public List<SimplifiedNamedColumn> getSelectedColumns() {
        List<SimplifiedNamedColumn> selectedColumns = Lists.newArrayList();
        NTableMetadataManager tableMetadata = null;
        if (!isBroken()) {
            tableMetadata = NTableMetadataManager.getInstance(getConfig(), this.getProject());
        }
        Set<String> excludedTables = loadExcludedTables();
        List<JoinTableDesc> joinTables = getLazyModel().getJoinTables();
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, joinTables, getLazyModel());
        for (NamedColumn namedColumn : getAllSelectedColumns()) {
            SimplifiedNamedColumn simplifiedNamedColumn = new SimplifiedNamedColumn(namedColumn);
            TblColRef colRef = findColumnByAlias(simplifiedNamedColumn.getAliasDotColumn());
            if (simplifiedNamedColumn.getStatus() == DIMENSION && colRef != null && tableMetadata != null) {
                if (checker.isColRefDependsLookupTable(colRef)) {
                    simplifiedNamedColumn.setDependLookupTable(true);
                }
                TableExtDesc tableExt = tableMetadata.getTableExtIfExists(colRef.getTableRef().getTableDesc());
                TableExtDesc.ColumnStats columnStats = Objects.isNull(tableExt) ? null
                        : tableExt.getColumnStatsByName(colRef.getName());
                if (columnStats != null) {
                    simplifiedNamedColumn.setCardinality(columnStats.getCardinality());
                }

            }
            selectedColumns.add(simplifiedNamedColumn);
        }

        return selectedColumns;
    }

    private Set<String> loadExcludedTables() {
        FavoriteRuleManager favoriteRuleManager = null;
        if (!isBroken()) {
            favoriteRuleManager = FavoriteRuleManager.getInstance(getConfig(), getProject());
        }
        Set<String> excludedTables = Sets.newHashSet();
        if (favoriteRuleManager != null) {
            excludedTables.addAll(favoriteRuleManager.getExcludedTables());
        }
        return excludedTables;
    }

    public void computedInfo(long inconsistentCount, ModelStatusToDisplayEnum status, boolean isScd2,
            NDataModel modelDesc, boolean onlyNormalDim) {
        if (!onlyNormalDim) {
            this.enrichDerivedDimension();
        }
        this.setForbiddenOnline(isScd2);
        this.setBroken(modelDesc.isBroken());
        this.setStatus(status);
        this.setInconsistentSegmentCount(inconsistentCount);
        computedDisplayInfo(modelDesc);
    }

    protected void computedDisplayInfo(NDataModel modelDesc) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        this.setLastBuildTime(dfManager.getDataflowLastBuildTime(modelDesc.getUuid()));
        this.setStorage(dfManager.getDataflowStorageSize(modelDesc.getUuid()));
        this.setSource(dfManager.getDataflowSourceSize(modelDesc.getUuid()));
        this.setSegmentHoles(dfManager.calculateSegHoles(modelDesc.getUuid()));
        this.setExpansionrate(ModelUtils.computeExpansionRate(this.getStorage(), this.getSource()));
        this.setUsage(dfManager.getDataflow(modelDesc.getUuid()).getQueryHitCount());
        if (!modelDesc.isBroken()) {
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelDesc.getUuid());
            this.setAvailableIndexesCount(indexPlanManager.getAvailableIndexesCount(getProject(), modelDesc.getId()));
            this.setTotalIndexes(indexPlan.getAllLayouts().size());
            this.setEmptyIndexesCount(this.totalIndexes - this.availableIndexesCount);
            this.setHasBaseAggIndex(indexPlan.containBaseAggLayout());
            this.setHasBaseTableIndex(indexPlan.containBaseTableLayout());
        }
    }
}