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
package io.kyligence.kap.metadata.cube.cuboid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AggIndexMatcher extends IndexMatcher {

    private final boolean isReplaceCount;
    private Set<Integer> sqlColumns;
    private final Map<FunctionDesc, List<Integer>> functionCols = Maps.newHashMap();
    private final boolean valid;

    public AggIndexMatcher(SQLDigest sqlDigest, ChooserContext model, Set<String> excludedTables,
            boolean isReplaceCount) {
        super(sqlDigest, model, excludedTables);
        this.isReplaceCount = isReplaceCount;
        valid = init();
    }

    private boolean init() {
        // cols may have null values as the CC col in query may not present in the model
        sqlColumns = Stream.concat(sqlDigest.filterColumns.stream(), sqlDigest.groupbyColumns.stream())
                .map(tblColMap::get).collect(Collectors.toSet());
        if (sqlColumns.contains(null)) {
            return false;
        }

        for (FunctionDesc agg : sqlDigest.aggregations) {
            List<Integer> cols = agg.getSourceColRefs().stream().map(tblColMap::get).collect(Collectors.toList());
            for (Integer col : cols) {
                if (col == null) {
                    return false;
                }
            }
            functionCols.put(agg, cols);
        }
        return true;
    }

    public boolean valid() {
        return valid;
    }

    @Override
    MatchResult match(LayoutEntity layout) {
        if (!needAggIndexMatch(layout.getIndex()) || !valid) {
            return new MatchResult(false);
        }
        log.trace("Matching agg index");
        Set<Integer> unmatchedCols = Sets.newHashSet();
        unmatchedCols.addAll(sqlColumns);
        Set<FunctionDesc> unmatchedMetrics = Sets.newHashSet(sqlDigest.aggregations);

        if (isBatchFusionModel) {
            unmatchedCols.removeAll(layout.getStreamingColumns().keySet());
        }
        unmatchedCols.removeAll(layout.getOrderedDimensions().keySet());
        final Map<Integer, DeriveInfo> needDerive = Maps.newHashMap();
        goThruDerivedDims(layout.getIndex(), needDerive, unmatchedCols);
        unmatchedAggregations(unmatchedMetrics, layout);
        if (isReplaceCount) {
            unmatchedCountColumnIfExistCountStar(unmatchedMetrics);
        }

        removeUnmatchedGroupingAgg(unmatchedMetrics);
        List<CapabilityResult.CapabilityInfluence> influences = Lists.newArrayList();
        if (!unmatchedMetrics.isEmpty() || !unmatchedCols.isEmpty()) {
            applyAdvanceMeasureStrategy(layout, unmatchedCols, unmatchedMetrics, influences);
            applyDimAsMeasureStrategy(layout, unmatchedMetrics, needDerive, influences);
        }

        boolean matched = unmatchedCols.isEmpty() && unmatchedMetrics.isEmpty();
        if (!matched && log.isDebugEnabled()) {
            log.debug("Agg index {} with unmatched columns {}, unmatched metrics {}", //
                    layout, unmatchedCols, unmatchedMetrics);
        }

        return new MatchResult(matched, needDerive, null, influences);
    }

    private boolean needAggIndexMatch(IndexEntity indexEntity) {
        return !indexEntity.isTableIndex() && !sqlDigest.isRawQuery;
    }

    private void removeUnmatchedGroupingAgg(Collection<FunctionDesc> unmatchedAggregations) {
        if (CollectionUtils.isEmpty(unmatchedAggregations))
            return;

        unmatchedAggregations
                .removeIf(functionDesc -> FunctionDesc.FUNC_GROUPING.equalsIgnoreCase(functionDesc.getExpression()));
    }

    private void unmatchedAggregations(Collection<FunctionDesc> aggregations, LayoutEntity cuboidLayout) {
        List<MeasureDesc> functionDescs = new ArrayList<>();
        if (isBatchFusionModel) {
            functionDescs.addAll(cuboidLayout.getStreamingMeasures().values());
        }
        functionDescs.addAll(cuboidLayout.getOrderedMeasures().values());

        for (MeasureDesc measureDesc : functionDescs) {
            aggregations.remove(measureDesc.getFunction());
        }
    }

    private void unmatchedCountColumnIfExistCountStar(Collection<FunctionDesc> aggregations) {
        aggregations.removeIf(FunctionDesc::isCountOnColumn);
    }

    private void applyDimAsMeasureStrategy(LayoutEntity layoutEntity, Collection<FunctionDesc> unmatchedAggs,
            Map<Integer, DeriveInfo> needDeriveCollector, List<CapabilityResult.CapabilityInfluence> influences) {
        IndexEntity indexEntity = layoutEntity.getIndex();
        Iterator<FunctionDesc> it = unmatchedAggs.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();
            if (functionDesc.isCountConstant()) {
                it.remove();
                continue;
            }

            // calcite can do aggregation from columns on-the-fly
            if (CollectionUtils.isEmpty(functionDesc.getParameters()))
                continue;

            Set<Integer> leftUnmatchedCols = Sets.newHashSet(functionCols.get(functionDesc));
            indexEntity.getDimensions().forEach(leftUnmatchedCols::remove);
            if (isBatchFusionModel) {
                leftUnmatchedCols.addAll(layoutEntity.getStreamingColumns().keySet());
            }
            if (CollectionUtils.isNotEmpty(leftUnmatchedCols)) {
                goThruDerivedDims(indexEntity, needDeriveCollector, leftUnmatchedCols);
            }

            if (CollectionUtils.isNotEmpty(leftUnmatchedCols))
                continue;

            if (FunctionDesc.DIMENSION_AS_MEASURES.contains(functionDesc.getExpression())) {
                influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                it.remove();
            }
        }
    }

    private void applyAdvanceMeasureStrategy(LayoutEntity layoutEntity, Collection<Integer> unmatchedDims,
            Collection<FunctionDesc> unmatchedMetrics, List<CapabilityResult.CapabilityInfluence> influences) {
        IndexEntity indexEntity = layoutEntity.getIndex();
        List<String> influencingMeasures = Lists.newArrayList();
        Set<NDataModel.Measure> measureSet = Sets.newHashSet(indexEntity.getMeasureSet());
        if (isBatchFusionModel) {
            measureSet.addAll(layoutEntity.getStreamingMeasures().values());
        }
        for (MeasureDesc measure : measureSet) {
            MeasureType measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof BasicMeasureType)
                continue;

            Set<TblColRef> dimRefs = new HashSet<>(chooserContext.convertToRefs(unmatchedDims));
            CapabilityResult.CapabilityInfluence inf = measureType.influenceCapabilityCheck(dimRefs, unmatchedMetrics,
                    sqlDigest, measure);
            // remove matched dims which disappears in dimRefs after measure matching
            unmatchedDims.removeIf(dim -> !dimRefs.contains(chooserContext.convertToRef(dim)));
            if (inf != null) {
                influences.add(inf);
                influencingMeasures.add(measure.getName() + "@" + measureType.getClass());
            }
        }
        if (!influencingMeasures.isEmpty()) {
            log.info("NDataflow {} CapabilityInfluences: {}", indexEntity.getIndexPlan().getUuid(),
                    StringUtils.join(influencingMeasures, ","));
        }
    }

}
