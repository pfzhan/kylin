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

package io.kyligence.kap.gtrecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.ScannerWorker;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.cube.raw.gridtable.RawTableScanRangePlanner;
import io.kyligence.kap.metadata.filter.EvaluatableFunctionTransformer;

public class RawTableSegmentScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(RawTableSegmentScanner.class);

    final GTScanRequest scanRequest;
    final ScannerWorker scanner;

    public RawTableSegmentScanner(RawTableSegment rawTableSegment, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter originalfilter, StorageContext context) {

        //the filter might be changed later in this SegmentScanner (In EvaluatableLikeFunctionTransformer)
        //to avoid issues like in https://issues.apache.org/jira/browse/KYLIN-1954, make sure each SegmentScanner
        //is working on its own copy
        byte[] serialize = TupleFilterSerializer.serialize(originalfilter, StringCodeSystem.INSTANCE);
        TupleFilter filter = TupleFilterSerializer.deserialize(serialize, StringCodeSystem.INSTANCE);

        //this is where CubeSegmentScanner does BuildInFunctionTransformer
        filter = EvaluatableFunctionTransformer.transform(filter);

        RawTableScanRangePlanner planner = new RawTableScanRangePlanner(rawTableSegment, filter, dimensions, groups, metrics);
        scanRequest = planner.planScanRequest();

        //TODO: set allow storage aggregation, set agg cache threshold, set limit, etc.

        scanner = new ScannerWorker(rawTableSegment, null, scanRequest, KapConfig.getInstanceFromEnv().getSparkRawTableGTStorage());
    }

    @Override
    public GTInfo getInfo() {
        return scanRequest == null ? null : scanRequest.getInfo();
    }

    @Override
    public long getScannedRowCount() {
        return scanner.getScannedRowCount();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return scanner.iterator();
    }
}
