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

package io.kyligence.kap.cube.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.model.IKapEngineAware;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;

@SuppressWarnings("serial")
public class NCubePlan extends RootPersistentEntity implements IEngineAware, IKeep {
    public static final String CUBE_PLAN_RESOURCE_ROOT = "/cube_plan";

    public static String concatResourcePath(String name) {
        return CUBE_PLAN_RESOURCE_ROOT + "/" + name + MetadataConstants.FILE_SURFIX;
    }

    // ============================================================================

    @JsonProperty("name")
    private String name;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("description")
    private String description;
    @JsonProperty("dimensions")
    private List<NDimensionDesc> dimensions = Lists.newArrayList();
    @JsonManagedReference
    @JsonProperty("cuboids")
    private List<NCuboidDesc> cuboids = Lists.newArrayList();
    @JsonProperty("override_properties")
    private LinkedHashMap<String, String> overrideProps = Maps.newLinkedHashMap();
    @JsonProperty("segment_range_start")
    private long segmentRangeStart = 0L;
    @JsonProperty("segment_range_end")
    private long segmentRangeEnd = Long.MAX_VALUE;
    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;
    @JsonProperty("retention_range")
    private long retentionRange = 0;
    @JsonProperty("notify_list")
    private List<String> notifyList = Lists.newArrayList();
    @JsonProperty("status_need_notify")
    private List<String> statusNeedNotify = Lists.newArrayList();
    @JsonProperty("engine_type")
    private int engineType = IKapEngineAware.ID_KAP_NSPARK;
    @JsonProperty("dictionaries")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<NDictionaryDesc> dictionaries;
    // computed fields below

    private String project;
    private KylinConfigExt config = null;
    private NDataModel model = null;

    private transient NSpanningTree spanningTree = null; // transient, because can self recreate
    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    private transient BiMap<Integer, NDataModel.Measure> effectiveMeasures; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable

    private LinkedHashSet<TblColRef> allColumns = Sets.newLinkedHashSet();
    private LinkedHashSet<ColumnDesc> allColumnDescs = Sets.newLinkedHashSet();
    private Map<Integer, NDimensionDesc.NEncodingDesc> dimEncodingMap = Maps.newLinkedHashMap();

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = Lists.newLinkedList();

    public NCubePlan() {
    }

    void initAfterReload(KylinConfig config) {
        checkArgument(StringUtils.isNotBlank(name), "NCubePlan name is blank");
        checkArgument(StringUtils.isNotBlank(modelName), "NCubePlan (%s) has blank model name", name);

        this.model = (NDataModel) NDataModelManager.getInstance(config, project).getDataModelDesc(modelName);
        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);

        // cube inherit the project override props
        Map<String, String> prjOverrideProps = ownerPrj.getOverrideKylinProps();
        for (Map.Entry<String, String> entry : prjOverrideProps.entrySet()) {
            if (!overrideProps.containsKey(entry.getKey())) {
                overrideProps.put(entry.getKey(), entry.getValue());
            }
        }
        this.config = KylinConfigExt.createInstance(config, overrideProps);

        checkNotNull(getModel(), "NDataModel(%s) not found", modelName);

        initAllCuboids();
        initDimensionAndMeasures();
        initAllColumns();
        initDimEncodings();
        initDictionaryDesc();
    }

    private void initAllCuboids() {
        for (NCuboidDesc cuboid : cuboids) {
            cuboid.init();
        }
    }

    private void initDimensionAndMeasures() {
        final BitSet dimBitSet = new BitSet();
        final BitSet measureBitSet = new BitSet();
        for (NCuboidDesc cuboid : cuboids) {
            dimBitSet.or(cuboid.getDimensionSet().mutable());
            measureBitSet.or(cuboid.getMeasureSet().mutable());
        }

        this.effectiveDimCols = Maps.filterKeys(model.getEffectiveColsMap(), new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer input) {
                return input != null && dimBitSet.get(input);
            }
        });
        this.effectiveMeasures = Maps.filterKeys(model.getEffectiveMeasureMap(), new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer input) {
                return input != null && measureBitSet.get(input);
            }
        });
    }

    private void initAllColumns() {
        allColumns.clear();
        allColumnDescs.clear();
        allColumns.addAll(effectiveDimCols.values());
        for (NDataModel.Measure measure : effectiveMeasures.values()) {
            allColumns.addAll(measure.getFunction().getParameter().getColRefs());
        }
        for (JoinTableDesc join : model.getJoinTables()) {
            CollectionUtils.addAll(allColumns, join.getJoin().getForeignKeyColumns());
            CollectionUtils.addAll(allColumns, join.getJoin().getPrimaryKeyColumns());
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

    private void initDimEncodings() {
        dimEncodingMap.clear();
        for (NDimensionDesc dimensionDesc : dimensions) {
            dimEncodingMap.put(dimensionDesc.getId(), dimensionDesc.getEncoding());
        }

        Preconditions.checkState(dimEncodingMap.keySet().containsAll(effectiveDimCols.keySet()),
                "Some dimensions do not have encoding configuration.");
    }

    @Override
    public String resourceName() {
        return name;
    }

    public NCubePlan copy() {
        return NCubePlanManager.getInstance(config, project).copy(this);
    }

    public NSpanningTree getSpanningTree() {
        if (spanningTree != null)
            return spanningTree;

        synchronized (this) {
            if (spanningTree == null) {
                spanningTree = NSpanningTreeFactory.fromCubePlan(this);
            }
            return spanningTree;
        }
    }

    public NCuboidDesc getCuboidDesc(int cuboidDescId) {
        return getSpanningTree().getCuboidDesc(cuboidDescId);
    }

    public NCuboidLayout getCuboidLayout(Long cuboidLayoutId) {
        return getSpanningTree().getCuboidLayout(cuboidLayoutId);
    }

    public KylinConfig getConfig() {
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

    public NDimensionDesc.NEncodingDesc getDimensionEncoding(TblColRef dimColRef) {
        return dimEncodingMap.get(model.getColId(dimColRef));
    }

    public String getResourcePath() {
        return new StringBuilder().append("/").append(project).append(CUBE_PLAN_RESOURCE_ROOT).append("/")
                .append(getName()).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public String getProject() {
        return project;
    }

    public void setProject(String projectName) {
        this.project = projectName;
    }

    public NDataModel getModel() {
        return model;
    }

    public BiMap<Integer, TblColRef> getEffectiveDimCols() {
        return effectiveDimCols;
    }

    public BiMap<Integer, NDataModel.Measure> getEffectiveMeasures() {
        return effectiveMeasures;
    }

    public Set<TblColRef> getAllColumnsHaveDictionary() {
        Set<TblColRef> result = Sets.newLinkedHashSet();

        // dictionaries in dimensions
        for (Map.Entry<Integer, TblColRef> dimEntry : effectiveDimCols.entrySet()) {
            NDimensionDesc.NEncodingDesc encodingDesc = dimEncodingMap.get(dimEntry.getKey());
            if (isUsingDictionary(encodingDesc.getEncodingName())) {
                result.add(dimEntry.getValue());
            }
        }

        // dictionaries in measures
        for (Map.Entry<Integer, NDataModel.Measure> measureEntry : effectiveMeasures.entrySet()) {
            FunctionDesc funcDesc = measureEntry.getValue().getFunction();
            MeasureType<?> aggrType = funcDesc.getMeasureType();
            result.addAll(aggrType.getColumnsNeedDictionary(funcDesc));
        }

        // any additional dictionaries
        if (dictionaries != null) {
            for (NDictionaryDesc dictDesc : dictionaries) {
                TblColRef col = dictDesc.getColumnRef();
                result.add(col);
            }
        }

        return result;
    }

    private boolean isUsingDictionary(String encodingName) {
        return DictionaryDimEnc.ENCODING_NAME.equals(encodingName);
    }

    /**
     * Get columns that need dictionary built on it. Note a column could reuse dictionary of another column.
     */
    public Set<TblColRef> getAllColumnsNeedDictionaryBuilt() {
        Set<TblColRef> result = getAllColumnsHaveDictionary();

        // remove columns that reuse other's dictionary
        if (dictionaries != null) {
            for (NDictionaryDesc dictDesc : dictionaries) {
                if (dictDesc.getResuseColumnRef() != null) {
                    result.remove(dictDesc.getColumnRef());
                    result.add(dictDesc.getResuseColumnRef());
                }
            }
        }

        return result;
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
            if (desc.getBuilderClass() != null) {
                // column that reuses other's dict need not be built, thus should not reach here
                if (col.equals(desc.getColumnRef())) {
                    return desc.getBuilderClass();
                }
            }
        }
        return null;
    }

    NCuboidDesc getLastCuboidDesc() {
        List<NCuboidDesc> existing = cuboids;
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    public Set<TblColRef> listAllColumns() {
        return allColumns;
    }

    public Set<ColumnDesc> listAllColumnDescs() {
        return allColumnDescs;
    }

    Set<TblColRef> listDimensionColumnsIncludingDerived() {
        return model.getEffectiveColsMap().values();
    }

    public List<FunctionDesc> listAllFunctions() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();
        for (MeasureDesc m : effectiveMeasures.values()) {
            functions.add(m.getFunction());
        }
        return functions;
    }

    public List<NCuboidLayout> getAllCuboidLayouts() {
        List<NCuboidLayout> r = new ArrayList<>();
        for (NCuboidDesc cd : cuboids) {
            r.addAll(cd.getLayouts());
        }
        return r;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public List<NCuboidDesc> getCuboids() {
        return isCachedAndShared ? ImmutableList.copyOf(cuboids) : cuboids;
    }

    public void setCuboids(List<NCuboidDesc> cuboids) {
        checkIsNotCachedAndShared();
        this.cuboids = cuboids;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        checkIsNotCachedAndShared();
        this.name = name;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        checkIsNotCachedAndShared();
        this.modelName = modelName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public List<NDimensionDesc> getDimensions() {
        return isCachedAndShared ? ImmutableList.copyOf(dimensions) : dimensions;
    }

    public void setDimensions(List<NDimensionDesc> dimensions) {
        checkIsNotCachedAndShared();
        this.dimensions = dimensions;
    }

    public LinkedHashMap<String, String> getOverrideProps() {
        return isCachedAndShared ? new LinkedHashMap(overrideProps) : overrideProps;
    }

    public void setOverrideProps(LinkedHashMap<String, String> overrideProps) {
        checkIsNotCachedAndShared();
        this.overrideProps = overrideProps;
    }

    public long getSegmentRangeStart() {
        return segmentRangeStart;
    }

    public void setSegmentRangeStart(long segmentRangeStart) {
        checkIsNotCachedAndShared();
        this.segmentRangeStart = segmentRangeStart;
    }

    public long getSegmentRangeEnd() {
        return segmentRangeEnd;
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

    public long getRetentionRange() {
        return retentionRange;
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

}
