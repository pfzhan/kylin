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

package io.kyligence.kap.engine.spark.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree;
import io.kyligence.kap.metadata.model.NDataModel;

@Slf4j
public class SegmentFlatTableDesc {
    protected final KylinConfig config;
    protected final KapConfig kapConfig;
    protected final NDataSegment dataSegment;
    protected final AdaptiveSpanningTree spanningTree;

    protected final String project;
    protected final String segmentId;
    protected final String dataflowId;
    protected final NDataModel dataModel;
    protected final IndexPlan indexPlan;

    private final IndexDependencyParser parser;

    // By design. Historical debt, wait for reconstruction.
    private final Map<String, Integer> columnIdMap = Maps.newHashMap();

    private final List<TblColRef> columns = Lists.newLinkedList();
    private final List<Integer> columnIds = Lists.newArrayList();
    private final Map<Integer, String> columnId2Canonical = Maps.newHashMap();
    private final Map<String, String> canonical2Table = Maps.newHashMap();

    private final List<String> relatedTables = Lists.newArrayList();

    public List<String> getRelatedTables() {
        return relatedTables;
    }

    public boolean isPartialBuild() {
        return !relatedTables.isEmpty();
    }

    public SegmentFlatTableDesc(KylinConfig config, NDataSegment dataSegment, AdaptiveSpanningTree spanningTree) {
        this(config, dataSegment, spanningTree, Lists.newArrayList());
    }

    public SegmentFlatTableDesc(KylinConfig config, NDataSegment dataSegment, AdaptiveSpanningTree spanningTree,
            List<String> relatedTables) {
        this.config = config;
        this.kapConfig = KapConfig.getInstanceFromEnv();
        this.dataSegment = dataSegment;
        this.spanningTree = spanningTree;

        this.project = dataSegment.getProject();
        this.segmentId = dataSegment.getId();
        this.dataflowId = dataSegment.getDataflow().getId();
        this.dataModel = dataSegment.getModel();
        this.indexPlan = dataSegment.getIndexPlan();
        this.relatedTables.addAll(relatedTables);
        this.parser = new IndexDependencyParser(dataModel);

        // Initialize flat table columns.
        initColumns();
    }

    public String getProject() {
        return project;
    }

    public NDataSegment getDataSegment() {
        return this.dataSegment;
    }

    public AdaptiveSpanningTree getSpanningTree() {
        return this.spanningTree;
    }

    public NDataModel getDataModel() {
        return this.dataModel;
    }

    public IndexPlan getIndexPlan() {
        return this.indexPlan;
    }

    public SegmentRange getSegmentRange() {
        return this.dataSegment.getSegRange();
    }

    public Path getFlatTablePath() {
        return config.getFlatTableDir(project, dataflowId, segmentId);
    }

    public Path getFactTableViewPath() {
        return config.getFactTableViewDir(project, dataflowId, segmentId);
    }

    public String getWorkingDir() {
        String workingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
        return StringUtils.removeEnd(workingDir, Path.SEPARATOR);
    }

    public int getSampleRowCount() {
        return config.getCapacitySampleRows();
    }

    // Flat table
    public boolean shouldPersistFlatTable() {
        return !isPartialBuild() && config.isPersistFlatTableEnabled();
    }

    // Fact table view
    public boolean shouldPersistView() {
        return config.isPersistFlatViewEnabled();
    }

    public String getColumnIdAsString(TblColRef colRef) {
        Integer id = columnIdMap.get(colRef.getIdentity());
        Preconditions.checkNotNull(id);
        return id.toString();
    }

    public List<TblColRef> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public List<Integer> getColumnIds() {
        return Collections.unmodifiableList(columnIds);
    }

    public Set<MeasureDesc> getMeasures() {
        return Collections.unmodifiableSet(indexPlan.getEffectiveMeasures().values());
    }

    public String getCanonicalName(Integer columnId) {
        return columnId2Canonical.get(columnId);
    }

    public String getTableName(String canonicalName) {
        return canonical2Table.get(canonicalName);
    }
    // Join lookup tables
    public boolean shouldJoinLookupTables() {

        if (!config.isFlatTableJoinWithoutLookup()) {
            return true;
        }

        if (StringUtils.isNotBlank(dataModel.getFilterCondition())) {
            return true;
        }
        final List<JoinTableDesc> joinTables = dataModel.getJoinTables();
        if (joinTables.stream().map(desc -> desc.getJoin().isLeftJoin()).count() != joinTables.size()) {
            return true;
        }

        if (joinTables.stream().map(desc -> desc.getKind() == NDataModel.TableKind.LOOKUP).count() != joinTables
                .size()) {
            return true;
        }

        final String factTableId = dataModel.getRootFactTable().getTableIdentity();
        return spanningTree.getLevel0thIndices().stream().anyMatch(index -> index.getEffectiveDimCols().values() //
                .stream().anyMatch(col -> !col.getTableRef().getTableIdentity().equalsIgnoreCase(factTableId)) //
                || index.getEffectiveMeasures().values().stream().anyMatch(m -> m.getFunction().getColRefs().stream() //
                        .anyMatch(col -> !col.getTableRef().getTableIdentity().equalsIgnoreCase(factTableId))));
    }

    public int getFlatTableCoalescePartitionNum() {
        return config.getFlatTableCoalescePartitionNum();
    }

    // Check what columns from hive tables are required, and index them
    protected void initColumns() {
        if (shouldPersistFlatTable()) {
            if (config.isIndexColumnFlatTableEnabled()) {
                addIndexPlanColumns();
            } else {
                addModelColumns();
            }
        } else if (isPartialBuild()) {
            addIndexPartialBuildColumns();
        } else {
            addIndexPlanColumns();
        }
    }

    private void addModelColumns() {
        // Add dimension columns
        dataModel.getEffectiveDimensions().values() //
                .stream().filter(Objects::nonNull) //
                .forEach(this::addColumn);
        // Add measure columns
        dataModel.getEffectiveMeasures().values().stream() //
                .filter(Objects::nonNull) //
                .filter(measure -> Objects.nonNull(measure.getFunction())) //
                .filter(measure -> Objects.nonNull(measure.getFunction().getColRefs())) //
                .flatMap(measure -> measure.getFunction().getColRefs().stream()) //
                .forEach(this::addColumn);
    }

    protected void addIndexPartialBuildColumns() {
        Set<Integer> dimSet = spanningTree.getIndices().stream() //
                .flatMap(layout -> layout.getDimensions().stream()) //
                .collect(Collectors.toSet());
        indexPlan.getEffectiveDimCols().entrySet().stream() //
                .filter(dimEntry -> dimSet.contains(dimEntry.getKey())) //
                .map(Map.Entry::getValue) //
                .filter(Objects::nonNull) //
                .forEach(this::addColumn);

        Set<Integer> measureSet = spanningTree.getIndices().stream() //
                .flatMap(layout -> layout.getMeasures().stream()) //
                .collect(Collectors.toSet());

        indexPlan.getEffectiveMeasures().entrySet().stream() //
                .filter(measureEntry -> measureSet.contains(measureEntry.getKey())) //
                .map(Map.Entry::getValue) //
                .filter(measure -> Objects.nonNull(measure.getFunction())) //
                .filter(measure -> Objects.nonNull(measure.getFunction().getColRefs())) //
                .flatMap(measure -> measure.getFunction().getColRefs().stream()) //
                .forEach(this::addColumn);
    }

    protected final void addIndexPlanColumns() {
        // Add dimension columns
        indexPlan.getEffectiveDimCols().values() //
                .stream().filter(Objects::nonNull) //
                .forEach(this::addColumn);
        // Add measure columns
        indexPlan.getEffectiveMeasures().values().stream() //
                .filter(Objects::nonNull) //
                .filter(measure -> Objects.nonNull(measure.getFunction())) //
                .filter(measure -> Objects.nonNull(measure.getFunction().getColRefs())) //
                .flatMap(measure -> measure.getFunction().getColRefs().stream()) //
                .forEach(this::addColumn);
    }

    protected final void addColumn(TblColRef colRef) {
        if (columnIdMap.containsKey(colRef.getIdentity())) {
            return;
        }
        if (dataSegment.getExcludedTables().contains(colRef.getTable())) {
            return;
        }

        columns.add(colRef);

        int id = dataModel.getColumnIdByColumnName(colRef.getIdentity());
        Preconditions.checkArgument(id != -1,
                "Column: " + colRef.getIdentity() + " is not in model: " + dataModel.getUuid());
        columnIdMap.put(colRef.getIdentity(), id);
        columnIds.add(id);
        columnId2Canonical.put(id, colRef.getCanonicalName());
        canonical2Table.put(colRef.getCanonicalName(), colRef.getTable());


        if (kapConfig.isSourceUsageUnwrapComputedColumn() && colRef.getColumnDesc().isComputedColumn()) {
            try {
                parser.unwrapComputeColumn(colRef.getExpressionInSourceDB()).forEach(this::addColumn);
            } catch (Exception e) {
                log.warn("UnWrap computed column {} in project {} model {} exception", colRef.getExpressionInSourceDB(),
                        dataModel.getProject(), dataModel.getAlias(), e);
            }
        }
    }
}
