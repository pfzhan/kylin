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

import javax.annotation.Nullable;

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
import org.apache.kylin.storage.gtrecord.StorageResponseGTScatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClientParams;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;

public class RawTableSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(RawTableSparkRPC.class);

    protected RawTableSegment rawTableSegment;
    protected Cuboid cuboid;
    protected GTInfo info;
    private SparkDriverClient client;

    public RawTableSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info) {
        this.rawTableSegment = (RawTableSegment) segment;
        this.cuboid = cuboid;
        this.info = info;
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
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {

        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());
        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());

        long startTime = System.currentTimeMillis();
        SparkDriverClientParams sparkDriverClientParams = new SparkDriverClientParams(KylinConfig.getInstanceFromEnv().getConfigAsString(), //
                RealizationType.INVERTED_INDEX.toString(), rawTableSegment.getRawTableInstance().getUuid(), rawTableSegment.getUuid(), "RawTable", // 
                scanRequest.getInfo().getMaxLength(), getRequiredParquetColumns(scanRequest) //
        );
        logger.info("Filter: {}" + scanRequest.getFilterPushDown());

        SparkJobProtos.SparkJobResponse jobResponse = client.submit(//
                scanRequest.toByteArray(), sparkDriverClientParams);
        logger.info("Time for the gRPC visit is " + (System.currentTimeMillis() - startTime));
        if (jobResponse.getSucceed()) {
            Iterable<byte[]> shardBytes = Iterables.transform(jobResponse.getShardBlobsList(), new Function<SparkJobProtos.SparkJobResponse.ShardBlob, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(@Nullable SparkJobProtos.SparkJobResponse.ShardBlob x) {
                    return x.toByteArray();
                }
            });
            return new StorageResponseGTScatter(info, shardBytes.iterator(), scanRequest.getColumns(), 0, scanRequest.getStoragePushDownLimit());
        } else {
            logger.error(jobResponse.getErrorMsg());
            throw new RuntimeException("RPC failed due to above reason");
        }
    }
}
