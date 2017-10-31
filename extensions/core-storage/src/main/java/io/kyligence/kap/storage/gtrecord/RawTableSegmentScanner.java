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

package io.kyligence.kap.storage.gtrecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.ScannerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.cube.raw.gridtable.RawTableScanRangePlanner;
import io.kyligence.kap.metadata.filter.EvaluatableFunctionTransformer;
import io.kyligence.kap.metadata.filter.TupleFilterSerializerRawTableExt;

public class RawTableSegmentScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(RawTableSegmentScanner.class);

    final GTScanRequest scanRequest;
    final ScannerWorker scanner;

    public RawTableSegmentScanner(RawTableSegment rawTableSegment, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter originalfilter, StorageContext context) {

        //the filter might be changed later in this SegmentScanner (In EvaluatableLikeFunctionTransformer)
        //to avoid issues like in https://issues.apache.org/jira/browse/KYLIN-1954, make sure each SegmentScanner
        //is working on its own copy
        byte[] serialize = TupleFilterSerializerRawTableExt.serialize(originalfilter, StringCodeSystem.INSTANCE);
        TupleFilter filter = TupleFilterSerializerRawTableExt.deserialize(serialize, StringCodeSystem.INSTANCE);

        //this is where CubeSegmentScanner does BuildInFunctionTransformer
        filter = EvaluatableFunctionTransformer.transform(filter);

        RawTableScanRangePlanner planner = new RawTableScanRangePlanner(rawTableSegment, filter, dimensions, groups, metrics, context);
        scanRequest = planner.planScanRequest();

        //TODO: set allow storage aggregation, set agg cache threshold, set limit, etc.

        scanner = new ScannerWorker(rawTableSegment, null, scanRequest, KapConfig.getInstanceFromEnv().getSparkRawTableGTStorage(), context);
    }

    @Override
    public GTInfo getInfo() {
        return scanRequest == null ? null : scanRequest.getInfo();
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
