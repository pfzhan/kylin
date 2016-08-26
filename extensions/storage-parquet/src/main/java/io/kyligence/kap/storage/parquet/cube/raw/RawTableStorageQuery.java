/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.storage.parquet.cube.raw;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.gtrecord.RawTableSegmentScanner;
import io.kyligence.kap.gtrecord.SequentialRawTableTupleIterator;

public class RawTableStorageQuery implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(RawTableStorageQuery.class);

    private RawTableInstance rawTableInstance;
    private RawTableDesc rawTableDesc;

    public RawTableStorageQuery(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
        this.rawTableDesc = rawTableInstance.getRawTableDesc();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        List<RawTableSegmentScanner> scanners = Lists.newArrayList();
        for (RawTableSegment rawTableSegment : rawTableInstance.getSegments(SegmentStatusEnum.READY)) {
            RawTableSegmentScanner scanner;
            if (rawTableSegment.getCubeSegment().getInputRecords() == 0) {
                if (!skipZeroInputSegment(rawTableSegment)) {
                    logger.warn("raw segment {} input record is 0, " + "it may caused by kylin failed to the job counter " + "as the hadoop history server wasn't running", rawTableSegment);
                } else {
                    logger.warn("raw segment {} input record is 0, skip it ", rawTableSegment);
                    continue;
                }
            }

            scanner = new RawTableSegmentScanner(rawTableSegment, dimensions, Collections.<TblColRef> emptySet(), Collections.<FunctionDesc> emptySet(), sqlDigest.filter, context);
            scanners.add(scanner);
        }
        return new SequentialRawTableTupleIterator(scanners, rawTableInstance, dimensions, metrics, returnTupleInfo, context);
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (TblColRef column : sqlDigest.allColumns) {
            dimensions.add(column);
        }
        //TODO
    }

    protected boolean skipZeroInputSegment(RawTableSegment segment) {
        return true;
    }

}
