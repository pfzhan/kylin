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

package io.kyligence.kap.metadata.cube.model;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.kyligence.kap.metadata.cube.model.IndexEntity.INDEX_ID_STEP;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
public class IndexPlan extends RootPersistentEntity implements Serializable, IEngineAware, IKeep {
    public static final String INDEX_PLAN_RESOURCE_ROOT = "/index_plan";

    @JsonProperty("description")
    @Getter
    private String description;

    @JsonProperty("index_plan_override_indexes")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<Integer, String> indexPlanOverrideIndexes = Maps.newHashMap();

    @Getter
    @JsonManagedReference
    @JsonProperty("rule_based_index")
    private NRuleBasedIndex ruleBasedIndex;

    @Getter
    @JsonManagedReference
    @JsonProperty("indexes")
    private List<IndexEntity> indexes = Lists.newArrayList();
    @JsonProperty("override_properties")
    private LinkedHashMap<String, String> overrideProps = Maps.newLinkedHashMap();

    @Getter
    @JsonProperty("segment_range_start")
    private long segmentRangeStart = 0L;
    @Getter
    @JsonProperty("segment_range_end")
    private long segmentRangeEnd = Long.MAX_VALUE;
    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;
    @Getter
    @JsonProperty("retention_range")
    private long retentionRange = 0;
    @JsonProperty("notify_list")
    private List<String> notifyList = Lists.newArrayList();
    @JsonProperty("status_need_notify")
    private List<String> statusNeedNotify = Lists.newArrayList();
    @JsonProperty("engine_type")
    private int engineType = IEngineAware.ID_KAP_NSPARK;
    @JsonProperty("dictionaries")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<NDictionaryDesc> dictionaries;
    @Getter
    @JsonProperty("next_aggregation_index_id")
    private long nextAggregationIndexId = 0;
    @Getter
    @JsonProperty("next_table_index_id")
    private long nextTableIndexId = IndexEntity.TABLE_INDEX_START_ID;

    // computed fields below
    @Setter
    private String project;

    @Setter
    private KylinConfigExt config = null;
    private long prjMvccWhenConfigInitted = -1;
    private long indexPlanMvccWhenConfigInitted = -1;

    private transient NSpanningTree spanningTree = null; // transient, because can self recreate
    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    private transient BiMap<Integer, NDataModel.Measure> effectiveMeasures; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable

    //TODO: should move allColumns and allColumnDescs to model? no need to exist in cubeplan
    private LinkedHashSet<TblColRef> allColumns = Sets.newLinkedHashSet();
    private LinkedHashSet<ColumnDesc> allColumnDescs = Sets.newLinkedHashSet();
    private Map<Integer, NEncodingDesc> dimEncodingMap = Maps.newHashMap();

    private List<LayoutEntity> ruleBasedLayouts = Lists.newArrayList();

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = Lists.newLinkedList();

    public void initAfterReload(KylinConfig config, String p) {
        this.project = p;
        initConfig4IndexPlan(config);

        checkNotNull(getModel(), "NDataModel(%s) not found", uuid);
        initAllCuboids();
        initDimensionAndMeasures();
        initAllColumns();
        initDictionaryDesc();

        this.setDependencies(calcDependencies());
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {
        val manager = NDataModelManager.getInstance(config, project);
        NDataModel dataModelDesc = manager.getDataModelDesc(getId());

        return Lists.newArrayList(dataModelDesc != null ? dataModelDesc
                : new MissingRootPersistentEntity(NDataModel.concatResourcePath(getId(), project)));
    }

    private void initConfig4IndexPlan(KylinConfig config) {

        Map<String, String> newOverrides = Maps.newLinkedHashMap(this.overrideProps);
        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);
        // cube inherit the project override props
        Map<String, String> prjOverrideProps = ownerPrj.getOverrideKylinProps();
        for (Map.Entry<String, String> entry : prjOverrideProps.entrySet()) {
            if (!newOverrides.containsKey(entry.getKey())) {
                newOverrides.put(entry.getKey(), entry.getValue());
            }
        }

        this.config = KylinConfigExt.createInstance(config, newOverrides);
        this.prjMvccWhenConfigInitted = ownerPrj.getMvcc();
        this.indexPlanMvccWhenConfigInitted = this.getMvcc();
    }

    private void initAllCuboids() {
        if (ruleBasedIndex != null) {
            ruleBasedIndex.init();
            ruleBasedLayouts.addAll(ruleBasedIndex.genCuboidLayouts());
        }
    }

    private void initDimensionAndMeasures() {
        val indexes = getAllIndexes();
        int size1 = 1;
        int size2 = 1;
        for (IndexEntity cuboid : indexes) {
            size1 = Math.max(cuboid.getDimensionBitset().size(), size1);
            size2 = Math.max(cuboid.getMeasureBitset().size(), size2);
        }

        final BitSet dimBitSet = new BitSet(size1);
        final BitSet measureBitSet = new BitSet(size2);

        for (IndexEntity cuboid : indexes) {
            dimBitSet.or(cuboid.getDimensionBitset().mutable());
            measureBitSet.or(cuboid.getMeasureBitset().mutable());
        }

        this.effectiveDimCols = Maps.filterKeys(getModel().getEffectiveColsMap(),
                input -> input != null && dimBitSet.get(input));
        this.effectiveMeasures = Maps.filterKeys(getModel().getEffectiveMeasureMap(),
                input -> input != null && measureBitSet.get(input));
    }

    private void initAllColumns() {
        allColumns.clear();
        allColumnDescs.clear();

        allColumns.addAll(effectiveDimCols.values());
        for (NDataModel.Measure measure : effectiveMeasures.values()) {
            allColumns.addAll(measure.getFunction().getColRefs());
        }
        for (JoinTableDesc join : getModel().getJoinTables()) {
            CollectionUtils.addAll(allColumns, join.getJoin().getForeignKeyColumns());
            //all lookup tables are automatically derived
            allColumns.addAll(join.getTableRef().getColumns());
        }

        for (TblColRef colRef : allColumns) {
            allColumnDescs.add(colRef.getColumnDesc());
        }
    }

    private void initDictionaryDesc() {
        if (dictionaries != null) {
            for (NDictionaryDesc dictDesc : dictionaries) {
                dictDesc.init(getModel());
                allColumns.add(dictDesc.getColumnRef());
                if (dictDesc.getResuseColumnRef() != null) {
                    allColumns.add(dictDesc.getResuseColumnRef());
                }
            }
        }
    }

    @Override
    public String resourceName() {
        return uuid;
    }

    public IndexPlan copy() {
        return NIndexPlanManager.getInstance(config, project).copy(this);
    }

    public NSpanningTree getSpanningTree() {
        if (spanningTree != null)
            return spanningTree;

        synchronized (this) {
            if (spanningTree == null) {
                spanningTree = NSpanningTreeFactory.fromIndexPlan(this);
            }
            return spanningTree;
        }
    }

    public IndexEntity getIndexEntity(long cuboidDescId) {
        return getSpanningTree().getIndexEntity(cuboidDescId);
    }

    public LayoutEntity getCuboidLayout(Long cuboidLayoutId) {
        return getSpanningTree().getCuboidLayout(cuboidLayoutId);
    }

    public KylinConfig getConfig() {
        if (config == null) {
            return null;
        }

        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);
        if (ownerPrj.getMvcc() != prjMvccWhenConfigInitted || this.getMvcc() != indexPlanMvccWhenConfigInitted) {
            initConfig4IndexPlan(this.config);
        }
        return config;
    }

    public void addError(String message) {
        this.errors.add(message);
    }

    public List<String> getError() {
        return this.errors;
    }

    String getErrorMsg() {
        return Joiner.on(" ").join(errors);
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(INDEX_PLAN_RESOURCE_ROOT).append("/").append(name)
                .append(MetadataConstants.FILE_SURFIX).toString();
    }

    public String getProject() {
        return project;
    }

    public NDataModel getModel() {
        return NDataModelManager.getInstance(config, project).getDataModelDesc(uuid);
    }

    public String getModelAlias() {
        val model = getModel();
        return model == null ? null : model.getAlias();
    }

    public BiMap<Integer, TblColRef> getEffectiveDimCols() {
        return effectiveDimCols;
    }

    public BiMap<Integer, NDataModel.Measure> getEffectiveMeasures() {
        return effectiveMeasures;
    }

    /**
     * A column may reuse dictionary of another column, find the dict column, return same col if there's no reuse column
     */
    TblColRef getDictionaryReuseColumn(TblColRef col) {
        if (dictionaries == null) {
            return col;
        }
        for (NDictionaryDesc dictDesc : dictionaries) {
            if (dictDesc.getColumnRef().equals(col) && dictDesc.getResuseColumnRef() != null) {
                return dictDesc.getResuseColumnRef();
            }
        }
        return col;
    }

    public String getDictionaryBuilderClass(TblColRef col) {
        if (dictionaries == null)
            return null;

        for (NDictionaryDesc desc : dictionaries) {
            // column that reuses other's dict need not be built, thus should not reach here
            if (desc.getBuilderClass() != null && col.equals(desc.getColumnRef())) {
                return desc.getBuilderClass();
            }
        }
        return null;
    }

    public Set<ColumnDesc> listAllColumnDescs() {
        return allColumnDescs;
    }

    public Set<TblColRef> listAllTblColRefs() {
        return allColumns;
    }

    public List<FunctionDesc> listAllFunctions() {
        List<FunctionDesc> functions = new ArrayList<>();
        for (MeasureDesc m : effectiveMeasures.values()) {
            functions.add(m.getFunction());
        }
        return functions;
    }

    public List<IndexEntity> getAllIndexes() {
        Map<Long, Integer> retSubscriptMap = Maps.newHashMap();
        List<IndexEntity> mergedIndexes = Lists.newArrayList();
        int retSubscript = 0;
        for (IndexEntity indexEntity : indexes) {
            val copy = JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class);
            retSubscriptMap.put(indexEntity.getId(), retSubscript);
            mergedIndexes.add(copy);
            retSubscript++;
        }
        for (LayoutEntity ruleBasedLayout : ruleBasedLayouts) {
            val ruleRelatedIndex = ruleBasedLayout.getIndex();
            if (!retSubscriptMap.containsKey(ruleRelatedIndex.getId())) {
                val copy = JsonUtil.deepCopyQuietly(ruleRelatedIndex, IndexEntity.class);
                retSubscriptMap.put(ruleRelatedIndex.getId(), retSubscript);
                mergedIndexes.add(copy);
                retSubscript++;
            }
            val subscript = retSubscriptMap.get(ruleRelatedIndex.getId());
            val targetIndex = mergedIndexes.get(subscript);
            val originLayouts = targetIndex.getLayouts();
            boolean isMatch = originLayouts.stream().filter(originLayout -> originLayout.equals(ruleBasedLayout))
                    .peek(originLayout -> originLayout.setManual(true)).count() > 0;
            LayoutEntity copyRuleBasedLayout;
            copyRuleBasedLayout = JsonUtil.deepCopyQuietly(ruleBasedLayout, LayoutEntity.class);
            if (!isMatch) {
                originLayouts.add(copyRuleBasedLayout);
            }
            targetIndex.setLayouts(originLayouts);
            targetIndex.setNextLayoutOffset(Math.max(targetIndex.getNextLayoutOffset(),
                    originLayouts.stream().mapToLong(LayoutEntity::getId).max().orElse(0) % INDEX_ID_STEP + 1));
            copyRuleBasedLayout.setIndex(targetIndex);
        }
        mergedIndexes.forEach(value -> value.setIndexPlan(this));
        return mergedIndexes;

    }

    public List<LayoutEntity> getAllLayouts() {
        List<LayoutEntity> r = Lists.newArrayList();

        for (IndexEntity cd : getAllIndexes()) {
            r.addAll(cd.getLayouts());
        }
        return r;
    }

    public List<LayoutEntity> getWhitelistLayouts() {
        List<LayoutEntity> r = Lists.newArrayList();

        for (IndexEntity cd : indexes) {
            r.addAll(cd.getLayouts());
        }
        return r;
    }

    public List<LayoutEntity> getRuleBaseLayouts() {
        return isCachedAndShared ? ImmutableList.copyOf(ruleBasedLayouts) : ruleBasedLayouts;
    }

    public Set<TblColRef> listDimensionColumnsIncludingDerived(IndexEntity cuboidDesc) {
        Set<TblColRef> ret = Sets.newHashSet();
        ret.addAll(effectiveDimCols.values());

        for (TblColRef col : effectiveDimCols.values()) {
            if (cuboidDesc != null && !cuboidDesc.dimensionsDerive(col)) {
                continue;
            }
            ret.add(col);
        }

        for (TableRef tableRef : getModel().getLookupTables()) {

            if (cuboidDesc != null) {
                JoinDesc joinByPKSide = getModel().getJoinByPKSide(tableRef);
                TblColRef[] fks = joinByPKSide.getForeignKeyColumns();
                if (!cuboidDesc.dimensionsDerive(fks)) {
                    continue;
                }
            }

            ret.addAll(tableRef.getColumns());
        }
        return ret;
    }

    public Set<TblColRef> listDimensionColumnsExcludingDerived(IndexEntity cuboidDesc) {
        Set<TblColRef> ret = Sets.newHashSet();
        ret.addAll(effectiveDimCols.values());

        for (TblColRef col : effectiveDimCols.values()) {
            if (cuboidDesc != null && !cuboidDesc.dimensionsDerive(col)) {
                continue;
            }
            ret.add(col);
        }

        return ret;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================
    public void setIndexes(List<IndexEntity> indexes) {
        checkIsNotCachedAndShared();
        this.indexes = indexes;
        updateNextId();
    }

    public void setRuleBasedIndex(NRuleBasedIndex nRuleBasedIndex, boolean reuseStartId) {
        checkIsNotCachedAndShared();
        nRuleBasedIndex.setMeasures(Lists.newArrayList(getModel().getEffectiveMeasureMap().keySet()));
        nRuleBasedIndex.setIndexStartId(reuseStartId ? nRuleBasedIndex.getIndexStartId() : nextAggregationIndexId);
        nRuleBasedIndex.setIndexPlan(this);
        nRuleBasedIndex.init();
        nRuleBasedIndex.genCuboidLayouts(
                this.ruleBasedIndex == null ? Sets.newHashSet() : this.ruleBasedIndex.genCuboidLayouts());
        this.ruleBasedIndex = nRuleBasedIndex;
        this.ruleBasedLayouts = Lists.newArrayList(this.ruleBasedIndex.genCuboidLayouts());
        updateNextId();
    }

    public void setRuleBasedIndex(NRuleBasedIndex ruleBasedIndex) {
        setRuleBasedIndex(ruleBasedIndex, false);
    }

    private void updateNextId() {
        val allIndexes = getAllIndexes();
        nextAggregationIndexId = Math.max(allIndexes.stream().filter(c -> !c.isTableIndex())
                .mapToLong(IndexEntity::getId).max().orElse(-INDEX_ID_STEP) + INDEX_ID_STEP, nextAggregationIndexId);
        nextTableIndexId = Math.max(allIndexes.stream().filter(IndexEntity::isTableIndex).mapToLong(IndexEntity::getId)
                .max().orElse(IndexEntity.TABLE_INDEX_START_ID - INDEX_ID_STEP) + INDEX_ID_STEP, nextTableIndexId);
        val indexNextIdMap = allIndexes.stream().collect(Collectors.toMap(IndexEntity::getId,
                k -> k.getLayouts().stream().mapToLong(LayoutEntity::getId).max().orElse(0) % INDEX_ID_STEP + 1));
        for (IndexEntity index : indexes) {
            long nextLayoutId = indexNextIdMap.getOrDefault(index.getId(), 1L);
            index.setNextLayoutOffset(Math.max(nextLayoutId, index.getNextLayoutOffset()));
        }
    }

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public ImmutableMap<Integer, String> getIndexPlanOverrideIndexes() {
        return ImmutableMap.copyOf(this.indexPlanOverrideIndexes);
    }

    public void setIndexPlanOverrideIndexes(Map<Integer, String> m) {
        checkIsNotCachedAndShared();
        this.indexPlanOverrideIndexes = m;
    }

    public LinkedHashMap<String, String> getOverrideProps() {
        return isCachedAndShared ? new LinkedHashMap(overrideProps) : overrideProps;
    }

    public void setOverrideProps(LinkedHashMap<String, String> overrideProps) {
        checkIsNotCachedAndShared();
        this.overrideProps = overrideProps;
    }

    public void setSegmentRangeStart(long segmentRangeStart) {
        checkIsNotCachedAndShared();
        this.segmentRangeStart = segmentRangeStart;
    }

    public void setSegmentRangeEnd(long segmentRangeEnd) {
        checkIsNotCachedAndShared();
        this.segmentRangeEnd = segmentRangeEnd;
    }

    public long[] getAutoMergeTimeRanges() {
        return isCachedAndShared ? Arrays.copyOf(autoMergeTimeRanges, autoMergeTimeRanges.length) : autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(long[] autoMergeTimeRanges) {
        checkIsNotCachedAndShared();
        this.autoMergeTimeRanges = autoMergeTimeRanges;
    }

    public void setRetentionRange(long retentionRange) {
        checkIsNotCachedAndShared();
        this.retentionRange = retentionRange;
    }

    public List<String> getNotifyList() {
        return isCachedAndShared ? ImmutableList.copyOf(notifyList) : notifyList;
    }

    public void setNotifyList(List<String> notifyList) {
        checkIsNotCachedAndShared();
        this.notifyList = notifyList;
    }

    public List<String> getStatusNeedNotify() {
        return isCachedAndShared ? ImmutableList.copyOf(statusNeedNotify) : statusNeedNotify;
    }

    /**
     * will ignore rule based indexes;
     */
    public Map<IndexEntity.IndexIdentifier, IndexEntity> getWhiteListIndexesMap() {
        return getIndexesMap(indexes);
    }

    /**
     * include rule based indexes
     */
    public Map<IndexEntity.IndexIdentifier, IndexEntity> getAllIndexesMap() {
        return getIndexesMap(getAllIndexes());
    }

    private Map<IndexEntity.IndexIdentifier, IndexEntity> getIndexesMap(List<IndexEntity> indexEntities) {
        Map<IndexEntity.IndexIdentifier, IndexEntity> originalCuboidsMap = Maps.newLinkedHashMap();
        for (IndexEntity cuboidDesc : indexEntities) {
            IndexEntity.IndexIdentifier identifier = cuboidDesc.createIndexIdentifier();
            if (!originalCuboidsMap.containsKey(identifier)) {
                originalCuboidsMap.put(identifier, cuboidDesc);
            } else {
                originalCuboidsMap.get(identifier).getLayouts().addAll(cuboidDesc.getLayouts());
            }
        }
        return isCachedAndShared ? ImmutableMap.copyOf(originalCuboidsMap) : originalCuboidsMap;
    }

    public void setStatusNeedNotify(List<String> statusNeedNotify) {
        checkIsNotCachedAndShared();
        this.statusNeedNotify = statusNeedNotify;
    }

    @Override
    public int getEngineType() {
        return engineType;
    }

    public void setEngineType(int engineType) {
        checkIsNotCachedAndShared();
        this.engineType = engineType;
    }

    public List<NDictionaryDesc> getDictionaries() {
        return dictionaries == null ? null
                : (isCachedAndShared ? ImmutableList.copyOf(dictionaries) : Collections.unmodifiableList(dictionaries));
    }

    public void setDictionaries(List<NDictionaryDesc> dictionaries) {
        checkIsNotCachedAndShared();
        this.dictionaries = dictionaries;
    }

    @Override
    public String toString() {
        return "IndexPlan [" + uuid + "(" + getModelAlias() + ")]";
    }

    /**
     * remove useless layouts from indexPlan without shared
     * this method will not persist indexPlan entity
     * @param cuboids the layouts to be removed, group by cuboid's identify
     * @param isSkip callback for user if skip some layout
     * @param equal compare if two layouts is equal
     * @param deleteAuto if delete auto layout
     * @param deleteManual if delete manual layout
     */
    public void removeLayouts(Map<IndexEntity.IndexIdentifier, List<LayoutEntity>> cuboids,
            Predicate<LayoutEntity> isSkip, BiPredicate<LayoutEntity, LayoutEntity> equal, boolean deleteAuto,
            boolean deleteManual) {
        checkIsNotCachedAndShared();
        Map<IndexEntity.IndexIdentifier, IndexEntity> originalCuboidsMap = getWhiteListIndexesMap();
        for (Map.Entry<IndexEntity.IndexIdentifier, List<LayoutEntity>> cuboidEntity : cuboids.entrySet()) {
            IndexEntity.IndexIdentifier cuboidKey = cuboidEntity.getKey();
            IndexEntity originalCuboid = originalCuboidsMap.get(cuboidKey);
            if (originalCuboid == null) {
                continue;
            }
            originalCuboid.removeLayoutsInCuboid(cuboidEntity.getValue(), isSkip, equal, deleteAuto, deleteManual);
            if (originalCuboid.getLayouts().isEmpty()) {
                originalCuboidsMap.remove(cuboidKey);
            }
        }

        setIndexes(Lists.newArrayList(originalCuboidsMap.values()));
    }

    public void removeLayouts(Map<IndexEntity.IndexIdentifier, List<LayoutEntity>> cuboidLayoutMap,
            BiPredicate<LayoutEntity, LayoutEntity> comparator, boolean deleteAuto, boolean deleteManual) {
        removeLayouts(cuboidLayoutMap, null, comparator, deleteAuto, deleteManual);
    }

    public void removeLayouts(Set<Long> cuboidLayoutIds, BiPredicate<LayoutEntity, LayoutEntity> comparator,
            boolean deleteAuto, boolean deleteManual) {
        val cuboidMap = Maps.newHashMap(getWhiteListIndexesMap());
        val toRemovedMap = Maps.<IndexEntity.IndexIdentifier, List<LayoutEntity>> newHashMap();
        for (Map.Entry<IndexEntity.IndexIdentifier, IndexEntity> cuboidDescEntry : cuboidMap.entrySet()) {
            val layouts = cuboidDescEntry.getValue().getLayouts();
            val filteredLayouts = Lists.<LayoutEntity> newArrayList();
            for (LayoutEntity layout : layouts) {
                if (cuboidLayoutIds.contains(layout.getId())) {
                    filteredLayouts.add(layout);
                }
            }

            toRemovedMap.put(cuboidDescEntry.getKey(), filteredLayouts);
        }

        removeLayouts(toRemovedMap, comparator, deleteAuto, deleteManual);
    }
}
