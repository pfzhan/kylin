/**
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

package io.kyligence.kap.storage.hbase;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.CubeScanRangePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.gridtable.GTScanRanges;
import io.kyligence.kap.cube.index.IIndexTable;

public class IndexGTScanRangePlanner extends CubeScanRangePlanner {
    private static final Logger logger = LoggerFactory.getLogger(IndexGTScanRangePlanner.class);
    private final IIndexTable indexTable;

    public IndexGTScanRangePlanner(CubeSegment cubeSegment, Cuboid cuboid, TupleFilter filter, Set<TblColRef> dimensions, Set<TblColRef> groupbyDims, Collection<FunctionDesc> metrics, StorageContext context) {
        super(cubeSegment, cuboid, filter, dimensions, groupbyDims, metrics, context);

        indexTable = new CubeSegmentIndexTable(cubeSegment, gtInfo, cuboid);
    }

    public List<GTScanRange> planScanRanges() {
        TupleFilter flatFilter = flattenToOrAndFilter(gtFilter);
        GTScanRanges scanRanges = translateToGTScanRanges(flatFilter);
        return scanRanges.getGTRangeList();
    }

    protected GTScanRanges translateToGTScanRanges(TupleFilter flatFilter) {
        GTScanRanges result = new GTScanRanges();

        if (flatFilter == null) {
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != TupleFilter.FilterOperatorEnum.AND)
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);

            GTScanRanges andRanges = translateToAndDimRanges(andFilter.getChildren());
            if (andRanges != null) {
                result = result.or(andRanges);
            }
        }

        return result;
    }

    private GTScanRanges translateToAndDimRanges(List<? extends TupleFilter> andFilters) {
        GTScanRanges result = null;
        for (TupleFilter filter : andFilters) {
            if ((filter instanceof CompareTupleFilter) == false) {
                if (filter instanceof ConstantTupleFilter && !filter.evaluate(null, null)) {
                    return null;
                } else {
                    continue;
                }
            }

            CompareTupleFilter comp = (CompareTupleFilter) filter;
            if (comp.getColumn() == null) {
                continue;
            }

            if (result == null) {
                result = indexTable.lookup(comp);
            } else {
                result = result.and(indexTable.lookup(comp));
            }
        }
        return result;
    }
}
