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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.ISegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SubmitParams;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.SparkResponseBlobGTScanner;

public class RawTableSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(RawTableSparkRPC.class);

    protected RawTableSegment rawTableSegment;
    private SparkDriverClient client;

    public RawTableSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info) {
        this.rawTableSegment = (RawTableSegment) segment;
        this.init();
    }

    protected void init() {
        try {
            client = new SparkDriverClient(KapConfig.getInstanceFromEnv().getSparkClientHost(), KapConfig.getInstanceFromEnv().getSparkClientPort());
        } catch (Exception e) {
            logger.error("error is " + e.getLocalizedMessage());
            throw e;
        }
    }

    protected List<Integer> getRequiredParquetColumns(GTScanRequest request) {
        List<Integer> ret = Lists.newArrayList();
        ImmutableBitSet immutableBitSet = request.getColumns();
        for (int i = 0; i < immutableBitSet.trueBitCount(); i++) {
            ret.add(immutableBitSet.trueBitAt(i));
        }
        return ret;
    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequests) throws IOException {

        long startTime = System.currentTimeMillis();
        SubmitParams submitParams = new SubmitParams(KylinConfig.getInstanceFromEnv().getConfigAsString(), //
                RealizationType.INVERTED_INDEX.toString(), rawTableSegment.getRawTableInstance().getUuid(), rawTableSegment.getUuid(), "RawTable", // 
                scanRequests.getInfo().getMaxLength(), getRequiredParquetColumns(scanRequests) //
        );
        logger.info("Filter: {}" + scanRequests.getFilterPushDown());

        SparkJobProtos.SparkJobResponse jobResponse = client.submit(//
                scanRequests.toByteArray(), submitParams);
        logger.info("Time for the gRPC visit is " + (System.currentTimeMillis() - startTime));
        if (jobResponse.getSucceed()) {
            return new SparkResponseBlobGTScanner(scanRequests, jobResponse.getGtRecordsBlob().toByteArray());
        } else {
            logger.error(jobResponse.getErrorMsg());
            throw new RuntimeException("RPC failed due to above reason");
        }
    }
}
