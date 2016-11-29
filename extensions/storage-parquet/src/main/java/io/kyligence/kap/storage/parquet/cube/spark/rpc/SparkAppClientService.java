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

package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse.ShardBlob;
import kap.google.common.base.Function;
import kap.google.common.base.Throwables;
import kap.google.common.collect.Iterables;
import kap.google.protobuf.ByteString;

//TODO: not thread safe now
public class SparkAppClientService implements JobServiceGrpc.JobService {

    public static final Logger logger = LoggerFactory.getLogger(SparkAppClientService.class);

    SparkConf conf;
    JavaSparkContext sc;

    public SparkAppClientService() {
        conf = new SparkConf().setAppName("Kyligence Columnar Storage Query Driver");
        //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.scheduler.mode", "FAIR");
        sc = new JavaSparkContext(conf);

        logger.info("Starting to warm up all executors");
        List<Integer> warmupData = new ArrayList<Integer>();
        for (int i = 0; i < 10000; i++) {
            warmupData.add(i);
        }
        sc.parallelize(warmupData).count();
        logger.info("Finish warming up all executors");
    }

    @Override
    public void submitJob(SparkJobRequest request, StreamObserver<SparkJobResponse> responseObserver) {

        String sparkInstanceIdentifer = System.getProperty("kap.spark.identifier");
        if (sparkInstanceIdentifer == null)
            sparkInstanceIdentifer = "";

        try {
            long startTime = System.currentTimeMillis();

            SparkParquetVisit submit = new SparkParquetVisit(sc, request);
            List<List<byte[]>> collected = submit.executeTask();

            logger.info("Time for spark parquet visit is " + (System.currentTimeMillis() - startTime));

            SparkJobResponse.Builder builder = SparkJobResponse.newBuilder().setSucceed(true);
            builder.addAllShardBlobs(Iterables.transform(collected, new Function<List<byte[]>, ShardBlob>() {
                @Nullable
                @Override
                public ShardBlob apply(@Nullable List<byte[]> bytes) {
                    return ShardBlob.newBuilder().setBlob(ByteString.copyFrom(concat(bytes))).build();
                }
            }));
            builder.setSparkInstanceIdentifier(sparkInstanceIdentifer);
            SparkJobResponse response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            String msg = Throwables.getStackTraceAsString(e);
            logger.error("error stacktrace: " + msg);

            SparkJobResponse response = SparkJobResponse.newBuilder().setSucceed(false).setErrorMsg(msg).setSparkInstanceIdentifier(sparkInstanceIdentifer).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static byte[] concat(List<byte[]> rows) {
        if (rows.size() == 1) {
            return rows.get(0);
        }

        int length = 0;
        for (byte[] row : rows) {
            length += row.length;
        }

        byte[] ret = new byte[length];
        int offset = 0;
        for (byte[] row : rows) {
            System.arraycopy(row, 0, ret, offset, row.length);
            offset += row.length;
        }
        return ret;
    }
}
