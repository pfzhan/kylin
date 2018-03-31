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

import org.apache.kylin.dict.BuiltInFunctionTransformer;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.CubeSegmentScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.storage.NDataStorageQuery;

public class NDataSegScanner implements IGTScanner {
    private static final Logger logger = LoggerFactory.getLogger(CubeSegmentScanner.class);

    final NDataSegment dataSegment;
    final NScannerWorker scanner;
    final NCuboidLayout cuboidLayout;

    final GTScanRequest scanRequest;

    public NDataSegScanner(NDataSegment dataSegment, NLayoutCandidate nLayoutCandidate, Set<TblColRef> dimensions,
            Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter originalfilter, TupleFilter havingFilter,
            StorageContext context) {
        logger.info("Init NDataSegScanner for segment {}", dataSegment.getName());

        this.cuboidLayout = nLayoutCandidate.getCuboidLayout();
        this.dataSegment = dataSegment;

        //the filter might be changed later in this NDataSegScanner (In ITupleFilterTransformer)
        //to avoid issues like in https://issues.apache.org/jira/browse/KYLIN-1954, make sure each NDataSegScanner
        //is working on its own copy
        byte[] serialize = TupleFilterSerializer.serialize(originalfilter, StringCodeSystem.INSTANCE);
        TupleFilter filter = TupleFilterSerializer.deserialize(serialize, StringCodeSystem.INSTANCE);

        // translate FunctionTupleFilter to IN clause
        ITupleFilterTransformer translator = new BuiltInFunctionTransformer(new NCubeDimEncMap(dataSegment));
        filter = translator.transform(filter);

        NDataflowScanRangePlanner scanRangePlanner;
        try {
            scanRangePlanner = new NDataflowScanRangePlanner(dataSegment, nLayoutCandidate, filter, dimensions, groups,
                    metrics, havingFilter, context);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        scanRequest = scanRangePlanner.planScanRequest();

        String gtStorage = ((NDataStorageQuery) context.getStorageQuery()).getGTStorage(cuboidLayout);
        scanner = new NScannerWorker(dataSegment, cuboidLayout, scanRequest, gtStorage, context);
    }

    public boolean isSegmentSkipped() {
        return scanner.isSegmentSkipped();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return scanner.iterator();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public GTInfo getInfo() {
        return scanRequest == null ? null : scanRequest.getInfo();
    }

    public GTScanRequest getScanRequest() {
        return scanRequest;
    }
}
