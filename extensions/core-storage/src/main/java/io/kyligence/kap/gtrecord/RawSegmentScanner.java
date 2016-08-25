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
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.cube.raw.gridtable.RawScanRangePlanner;

public class RawSegmentScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(RawSegmentScanner.class);

    final GTScanRequest scanRequest;
    final ScannerWorker scanner;

    public RawSegmentScanner(RawTableSegment rawTableSegment, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter filter, StorageContext context) {
        //CubeSegmentScanner will do BuildInFunctionTransformer here, but it's not necessary for this

        RawScanRangePlanner planner = new RawScanRangePlanner(rawTableSegment, filter, dimensions, groups, metrics);
        scanRequest = planner.planScanRequest();

        //TODO: set allow storage aggregation, set agg cache threshold, set limit, etc.
        if (scanRequest != null) {
            scanRequest.setAllowStorageAggregation(false);
        }

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
