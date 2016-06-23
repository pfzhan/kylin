/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package io.kyligence.kap.cube.gridtable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRangePlanner;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.ScannerWorker;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AdvGTScanRangePlanner extends GTScanRangePlanner {
    private static final Logger logger = LoggerFactory.getLogger(AdvGTScanRangePlanner.class);

    public AdvGTScanRangePlanner(CubeSegment cubeSegment, Cuboid cuboid, TupleFilter filter, Set<TblColRef> dimensions, Set<TblColRef> groupbyDims, Collection<FunctionDesc> metrics) {
        super(cubeSegment, cuboid, filter, dimensions, groupbyDims, metrics);
    }

    /**
     * use the "prefix cuboid" as an index
     *
     * @return
     */
    public List<GTScanRange> planScanRanges() {

        Cuboid prefixCuboid;

        List<TblColRef> dims = cuboid.getColumns();
        boolean inPrefix = false;
        long cuboidID = 0;
        for (int i = dims.size() - 1; i >= 0; i--) {
            if (filterDims.contains(dims.get(i))) {
                inPrefix = true;
            }
            if (inPrefix) {
                int index = cubeDesc.getRowkey().getColumnBitIndex(dims.get(i));
                cuboidID |= 1L << index;
            }
        }
        Cuboid temp = Cuboid.findById(cubeDesc, cuboidID);
        if (cuboidID != 0 && temp.getId() == cuboidID && temp.getId() != cuboid.getId()) {
            prefixCuboid = temp;
        } else {
            return super.planScanRanges();
        }

        logger.info("Cuboid Index {} is leveraged when dealing with cuboid {}", prefixCuboid.getId(), cuboid.getId());

        List<TblColRef> cuboidColumns = prefixCuboid.getColumns();
        //Set<TblColRef> newDimensions = Sets.newHashSet(dimensions);
        //newDimensions.retainAll(cuboidColumns);
        final Set<TblColRef> newDimensions = Sets.newHashSet(cuboidColumns);
        Set<TblColRef> newGroupByDims = Sets.newHashSet(groupbyDims);
        newGroupByDims.retainAll(cuboidColumns);

        //TODO: don't need the metrics
        GTScanRangePlanner scanRangePlanner = new GTScanRangePlanner(cubeSegment, prefixCuboid, filter, newDimensions, newDimensions, metrics);
        GTScanRequest scanRequest = scanRangePlanner.planScanRequest();
        ScannerWorker scanner = new ScannerWorker(cubeSegment, prefixCuboid, scanRequest, KylinConfig.getInstanceFromEnv().getDefaultIGTStorage());

        final ByteArray empty = new ByteArray();
        Iterator<GTRecord> itr = Iterators.transform(scanner.iterator(), new Function<GTRecord, GTRecord>() {
            @Nullable
            @Override
            public GTRecord apply(@Nullable GTRecord input) {
                //input is from prefix cuboid, while output is from target cuboid 

                ByteArray[] byteArrays = new ByteArray[gtInfo.getColumnCount()];
                ImmutableBitSet cuboidIndexPK = input.getInfo().getPrimaryKey();
                ImmutableBitSet PK = gtInfo.getPrimaryKey();
                for (int i = 0; i < PK.trueBitCount(); i++) {
                    int index = PK.trueBitAt(i);
                    if (cuboidIndexPK.get(i)) {
                        byteArrays[index] = input.get(index).copy();
                    } else {
                        byteArrays[index] = empty;
                    }
                }
                return new GTRecord(gtInfo, byteArrays);
            }
        });

        Set<GTRecord> sorted = Sets.newTreeSet();
        while (itr.hasNext()) {
            sorted.add(itr.next());
        }

        List<GTScanRange> ret = Lists.newArrayList(Iterators.transform(Iterators.transform(sorted.iterator(), new Function<GTRecord, GTRecord>() {
            @Nullable
            @Override
            public GTRecord apply(@Nullable GTRecord input) {
                return new GTRecord(gtInfo, input.getInternal());
            }
        }), new Function<GTRecord, GTScanRange>() {
            @Nullable
            @Override
            public GTScanRange apply(@Nullable GTRecord input) {
                return new GTScanRange(input, input, null);
            }
        }));

        if (ret.size() > maxScanRanges) {
            return super.planScanRanges();
        } else {
            return ret;
        }
    }
}
