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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class NDataflowCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowCapabilityChecker.class);

    public static CapabilityResult check(NDataflow dataflow, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.capable = false;

        // 1. match joins is ensured at model select

        // 2. ensure all dimensions and measures included in dataflow
        Collection<TblColRef> dimensionColumns = getDimensionColumns(digest);
        Collection<FunctionDesc> aggrFunctions = digest.aggregations;
        Collection<TblColRef> unmatchedDimensions = unmatchedDimensions(dimensionColumns, dataflow);
        Collection<FunctionDesc> unmatchedAggregations = unmatchedAggregations(aggrFunctions, dataflow);

        // try custom measure types
        tryCustomMeasureTypes(unmatchedDimensions, unmatchedAggregations, digest, dataflow, result);

        //more tricks
        String rootFactTable = dataflow.getModel().getRootFactTableName();
        if (rootFactTable.equals(digest.factTable)) {
            //for query-on-facttable
            //1. dimension as measure

            if (!unmatchedAggregations.isEmpty()) {
                tryDimensionAsMeasures(unmatchedAggregations, result,
                        dataflow.getCubePlan().listDimensionColumnsIncludingDerived(null));
            }
        } else {
            //for non query-on-facttable
            if (dataflow.getSegments().get(0).getSnapshots().containsKey(digest.factTable)) {

                Set<TblColRef> dimCols = Sets
                        .newHashSet(dataflow.getModel().findFirstTable(digest.factTable).getColumns());

                //1. all aggregations on lookup table can be done. For distinct count, mark them all DimensionAsMeasures
                // so that the measure has a chance to be upgraded to DimCountDistinctMeasureType in org.apache.kylin.metadata.model.FunctionDesc#reInitMeasureType
                if (!unmatchedAggregations.isEmpty()) {
                    Iterator<FunctionDesc> itr = unmatchedAggregations.iterator();
                    while (itr.hasNext()) {
                        FunctionDesc functionDesc = itr.next();
                        if (dimCols.containsAll(functionDesc.getParameter().getColRefs())) {
                            itr.remove();
                        }
                    }
                }
                tryDimensionAsMeasures(Lists.newArrayList(aggrFunctions), result, dimCols);

                //2. more "dimensions" contributed by snapshot
                if (!unmatchedDimensions.isEmpty()) {
                    unmatchedDimensions.removeAll(dimCols);
                }
            } else {
                logger.info("NDataflow {} does not touch lookup table {} at all", dataflow.getName(), digest.factTable);
                return result;
            }
        }

        if (!unmatchedDimensions.isEmpty()) {
            logger.info("Exclude NDataflow " + dataflow.getName() + " because unmatched dimensions: "
                    + unmatchedDimensions);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedDimensions(unmatchedDimensions);
            return result;
        }

        if (!unmatchedAggregations.isEmpty()) {
            logger.info("Exclude NDataflow " + dataflow.getName() + " because unmatched aggregations: "
                    + unmatchedAggregations);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedAggregations(unmatchedAggregations);
            return result;
        }

        if (dataflow.getStorageType() == IStorageAware.ID_HBASE
                && MassInTupleFilter.containsMassInTupleFilter(digest.filter)) {
            logger.info("Exclude NDataflow " + dataflow.getName()
                    + " because only v2 storage + v2 query engine supports massin");
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.UNSUPPORT_MASSIN);
            return result;
        }

        if (digest.limitPrecedesAggr) {
            logger.info("Exclude NDataflow " + dataflow.getName() + " because there's limit preceding aggregation");
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        if (digest.isRawQuery && rootFactTable.equals(digest.factTable)) {
            if (dataflow.getConfig().isDisableCubeNoAggSQL()) {
                result.incapableCause = CapabilityResult.IncapableCause
                        .create(CapabilityResult.IncapableType.UNSUPPORT_RAWQUERY);
                return result;
            } else {
                result.influences.add(new CapabilityResult.CapabilityInfluence() {
                    @Override
                    public double suggestCostMultiplier() {
                        return 100;
                    }

                    @Override
                    public MeasureDesc getInvolvedMeasure() {
                        return null;
                    }
                });
            }
        }

        // TODO: 3. ensure the wanted cuboid exists

        // cost will be minded by caller
        result.capable = true;
        return result;
    }

    private static Collection<TblColRef> getDimensionColumns(SQLDigest sqlDigest) {
        Collection<TblColRef> groupByColumns = sqlDigest.groupbyColumns;
        Collection<TblColRef> filterColumns = sqlDigest.filterColumns;

        Collection<TblColRef> dimensionColumns = new HashSet<TblColRef>();
        dimensionColumns.addAll(groupByColumns);
        dimensionColumns.addAll(filterColumns);
        return dimensionColumns;
    }

    private static Set<TblColRef> unmatchedDimensions(Collection<TblColRef> dimensionColumns, NDataflow dataflow) {
        HashSet<TblColRef> result = Sets.newHashSet(dimensionColumns);
        result.removeAll(dataflow.getCubePlan().listDimensionColumnsIncludingDerived(null));
        return result;
    }

    private static Set<FunctionDesc> unmatchedAggregations(Collection<FunctionDesc> aggregations, NDataflow dataflow) {
        HashSet<FunctionDesc> result = Sets.newHashSet(aggregations);
        result.removeAll(dataflow.getCubePlan().listAllFunctions());
        return result;
    }

    private static void tryDimensionAsMeasures(Collection<FunctionDesc> unmatchedAggregations, CapabilityResult result,
            Set<TblColRef> dimCols) {

        Iterator<FunctionDesc> it = unmatchedAggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();

            // let calcite handle count
            if (functionDesc.isCount()) {
                it.remove();
                continue;
            }

            // calcite can do aggregation from columns on-the-fly
            ParameterDesc parameterDesc = functionDesc.getParameter();
            if (parameterDesc == null) {
                continue;
            }
            List<TblColRef> neededCols = parameterDesc.getColRefs();
            if (neededCols.size() > 0 && dimCols.containsAll(neededCols)
                    && FunctionDesc.BUILT_IN_AGGREGATIONS.contains(functionDesc.getExpression())) {
                result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                it.remove();
                continue;
            }
        }
    }

    // custom measure types can cover unmatched dimensions or measures
    private static void tryCustomMeasureTypes(Collection<TblColRef> unmatchedDimensions,
            Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, NDataflow dataflow,
            CapabilityResult result) {
        List<String> influencingMeasures = Lists.newArrayList();
        for (MeasureDesc measure : dataflow.getMeasures()) {
            //            if (unmatchedDimensions.isEmpty() && unmatchedAggregations.isEmpty())
            //                break;

            MeasureType<?> measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof BasicMeasureType)
                continue;

            CapabilityResult.CapabilityInfluence inf = measureType.influenceCapabilityCheck(unmatchedDimensions,
                    unmatchedAggregations, digest, measure);
            if (inf != null) {
                result.influences.add(inf);
                influencingMeasures.add(measure.getName() + "@" + measureType.getClass());
            }
        }
        if (influencingMeasures.size() != 0)
            logger.info("NDataflow {} CapabilityInfluences: {}", dataflow.getCanonicalName(),
                    StringUtils.join(influencingMeasures, ","));
    }
}