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

package io.kyligence.kap.modeling.auto.mockup;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class MockupStorageQuery implements IStorageQuery {
    private static final Logger logger = LoggerFactory.getLogger(MockupStorageQuery.class);

    private final CubeDesc cubeDesc;
    private final CubeInstance cubeInstance;

    public MockupStorageQuery(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext storageContext, SQLDigest sqlDigest, TupleInfo tupleInfo) {
        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        Collection<TblColRef> filters = sqlDigest.filterColumns;

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        // all dimensions = groups + other(like filter) dimensions
        Set<TblColRef> otherDims = Sets.newHashSet(dimensions);
        otherDims.removeAll(groups);

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(groups, derivedPostAggregation);
        Set<TblColRef> filterD = expandDerived(filters, derivedPostAggregation);
        Set<TblColRef> otherDimsD = expandDerived(otherDims, derivedPostAggregation);
        otherDimsD.removeAll(groupsD);

        Set<TblColRef> dimensionsD = new LinkedHashSet<>();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(otherDimsD);
        final Cuboid cuboid = Cuboid.identifyCuboid(cubeDesc, dimensionsD, metrics);
        //final String cuboidBin = Long.toBinaryString(cuboid.getId());
        Iterable<Long> groupByCols = Iterables.transform(groupsD, new Function<TblColRef, Long>() {
            @Override
            public Long apply(@Nullable TblColRef tblColRef) {
                return 1L << cubeDesc.getRowkey().getColumnBitIndex(tblColRef);
            }
        });
        Iterable<Long> filterCols = Iterables.transform(filterD, new Function<TblColRef, Long>() {
            @Override
            public Long apply(@Nullable TblColRef tblColRef) {
                return 1L << cubeDesc.getRowkey().getColumnBitIndex(tblColRef);
            }
        });

        String uuid = storageContext.getConnUrl();
        QueryResult queryResult = MockupQueryRecorder.get(uuid);
        if (queryResult == null) {
            queryResult = new QueryResult();
            MockupQueryRecorder.addQueryResult(uuid, queryResult);
        }
        QueryResult.OLAPQueryResult olapQueryResult = new QueryResult.OLAPQueryResult();
        olapQueryResult.setCuboidId(cuboid.getId());
        olapQueryResult.setCubeName(cubeInstance.getName());
        olapQueryResult.setFilter(Sets.newHashSet(filterCols));
        olapQueryResult.setGroupBy(Sets.newHashSet(groupByCols));
        olapQueryResult.setMeasures(Sets.newHashSet(metrics));
        queryResult.addOLAPQuery(olapQueryResult);

        return ITupleIterator.EMPTY_TUPLE_ITERATOR;
    }

    private Set<TblColRef> expandDerived(Collection<TblColRef> cols, Set<TblColRef> derivedPostAggregation) {
        Set<TblColRef> expanded = Sets.newHashSet();
        for (TblColRef col : cols) {
            if (cubeDesc.hasHostColumn(col)) {
                CubeDesc.DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                for (TblColRef hostCol : hostInfo.columns) {
                    expanded.add(hostCol);
                    if (hostInfo.isOneToOne == false)
                        derivedPostAggregation.add(hostCol);
                }
            } else {
                expanded.add(col);
            }
        }
        return expanded;
    }

    private FunctionDesc findAggrFuncFromCubeDesc(FunctionDesc aggrFunc) {
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                metrics.add(findAggrFuncFromCubeDesc(func));
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column) && !(sqlDigest.groupbyColumns.contains(column) || sqlDigest.filterColumns.contains(column))) {
                continue;
            }

            dimensions.add(column);
        }
    }
}
