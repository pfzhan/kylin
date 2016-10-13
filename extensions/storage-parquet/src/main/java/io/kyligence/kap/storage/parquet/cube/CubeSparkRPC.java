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

package io.kyligence.kap.storage.parquet.cube;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.ISegment;
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

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClientParams;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;

public class CubeSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(CubeSparkRPC.class);

    protected CubeSegment cubeSegment;
    protected Cuboid cuboid;
    protected GTInfo info;

    private SparkDriverClient client;

    public CubeSparkRPC(ISegment segment, Cuboid cuboid, GTInfo info) {
        this.cubeSegment = (CubeSegment) segment;
        this.cuboid = cuboid;
        this.info = info;

        init();
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
        List<Integer> measures = Lists.newArrayList(0);//the row key parquet column
        int numDim = request.getInfo().getPrimaryKey().trueBitCount();
        for (int i = 0; i < request.getAggrMetrics().trueBitCount(); i++) {
            int index = request.getAggrMetrics().trueBitAt(i);
            measures.add(index - numDim + 1);
        }
        return measures;

    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {

        scanRequest.setTimeout(KapConfig.getInstanceFromEnv().getSparkVisitTimeout());
        logger.info("Spark visit timeout is set to " + scanRequest.getTimeout());

        long startTime = System.currentTimeMillis();
        SparkDriverClientParams sparkDriverClientParams = new SparkDriverClientParams(KylinConfig.getInstanceFromEnv().getConfigAsString(), //
                RealizationType.CUBE.toString(), cubeSegment.getCubeInstance().getUuid(), cubeSegment.getUuid(), String.valueOf(cuboid.getId()), // 
                scanRequest.getInfo().getMaxLength(), getRequiredParquetColumns(scanRequest) //
        );
        logger.info("Filter: {}", scanRequest.getFilterPushDown());

        SparkJobProtos.SparkJobResponse jobResponse = client.submit(scanRequest.toByteArray(), sparkDriverClientParams);
        logger.info("Time for the gRPC visit is " + (System.currentTimeMillis() - startTime));
        if (jobResponse.getSucceed()) {
            Iterable<byte[]> shardBytes = Iterables.transform(jobResponse.getShardBlobsList(), new Function<SparkJobProtos.SparkJobResponse.ShardBlob, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(@Nullable SparkJobProtos.SparkJobResponse.ShardBlob x) {
                    return x.getBlob().toByteArray();
                }
            });
            return new StorageResponseGTScatter(info, shardBytes.iterator(), scanRequest.getColumns(), 0, scanRequest.getStoragePushDownLimit());
        } else {
            logger.error(jobResponse.getErrorMsg());
            throw new RuntimeException("RPC failed due to: " + jobResponse.getErrorMsg());
        }
    }
}
