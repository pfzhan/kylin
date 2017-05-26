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
import java.util.concurrent.Semaphore;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.kyligence.kap.common.obf.IKeepClassMembers;
import io.kyligence.kap.storage.parquet.adhoc.SparkSqlClient;
import io.kyligence.kap.storage.parquet.adhoc.util.KapAdHocUtil;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;

public class SparkAppClientService extends JobServiceGrpc.JobServiceImplBase implements IKeepClassMembers {

    public static final Logger logger = LoggerFactory.getLogger(SparkAppClientService.class);

    private SparkConf conf;
    private JavaSparkContext sc;
    private Semaphore semaphore;

    public SparkAppClientService() {
        conf = new SparkConf().setAppName("KAP Query Driver");
        conf.set("spark.scheduler.mode", "FAIR");
        sc = new JavaSparkContext(conf);
        semaphore = new Semaphore((int) KapAdHocUtil.memoryStringToMegas(this.conf.get("spark.driver.memory")) / 2);

        logger.info("Starting to warm up all executors");
        List<Integer> warmupData = new ArrayList<Integer>();
        for (int i = 0; i < 10000; i++) {
            warmupData.add(i);
        }
        sc.parallelize(warmupData).count();
        logger.info("Finish warming up all executors");
    }

    @Override
    public StreamObserver<SparkJobRequest> submitJob(final StreamObserver<SparkJobResponse> responseObserver) {
        if (sc.sc().isStopped()) {
            logger.warn("Current JavaSparkContext(started at {} GMT) is found to be stopped, creating a new one",
                    DateFormat.formatToTimeStr(sc.startTime()));
            sc = new JavaSparkContext(conf);
            semaphore = new Semaphore((int) KapAdHocUtil.memoryStringToMegas(this.conf.get("spark.driver.memory")) / 2);
        }

        return new ServerStreamObserver(responseObserver, sc);
    }

    @Override
    public void doAdHocQuery(SparkJobProtos.AdHocRequest request,
            StreamObserver<SparkJobProtos.AdHocResponse> responseObserver) {
        if (sc.sc().isStopped()) {
            logger.warn("Current JavaSparkContext(started at {} GMT) is found to be stopped, creating a new one",
                    DateFormat.formatToTimeStr(sc.startTime()));
            sc = new JavaSparkContext(conf);
            semaphore = new Semaphore((int) KapAdHocUtil.memoryStringToMegas(this.conf.get("spark.driver.memory")) / 2);
        }

        logger.info("Starting to do ad hoc query");
        try {
            SparkSqlClient sqlClient = new SparkSqlClient(sc, request, semaphore);
            Pair<List<SparkJobProtos.Row>, List<SparkJobProtos.StructField>> pair = sqlClient.executeSql();

            responseObserver.onNext(SparkJobProtos.AdHocResponse.newBuilder().addAllRows(pair.getFirst())
                    .addAllColumns(pair.getSecond()).build());
            responseObserver.onCompleted();

            semaphore.release((int) sqlClient.getEstimateDfSize());

        } catch (Exception e) {
            logger.error("Ad Hoc Query Error:", e);
            throw new StatusRuntimeException(Status.INTERNAL
                    .withDescription("Ad hoc query not supported, please check spark-driver.log for details."));
        }
    }
}
