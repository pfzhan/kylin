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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;
import kap.google.protobuf.ByteString;

public class SparkDriverClient {
    private static final Logger logger = LoggerFactory.getLogger(SparkDriverClient.class);

    private final ManagedChannel channel;
    private final JobServiceGrpc.JobServiceBlockingStub blockingStub;

    public SparkDriverClient(String host, int port) {
        logger.info("SparkDriverClient host:" + host);
        logger.info("SparkDriverClient port:" + port);

        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
        blockingStub = JobServiceGrpc.newBlockingStub(channel);

        logger.info("finish ctor");
    }

    public SparkJobResponse submit(byte[] gtScanReq, SparkDriverClientParams sparkDriverClientParams) {
        SparkJobRequest request = SparkJobRequest.newBuilder().setGtScanRequest(ByteString.copyFrom(gtScanReq)).//
                setKylinProperties(sparkDriverClientParams.getKylinProperties()).setRealizationId(sparkDriverClientParams.getRealizationId()).//
                setSegmentId(sparkDriverClientParams.getSegmentId()).setDataFolderName(sparkDriverClientParams.getCuboidId()).//
                setMaxRecordLength(sparkDriverClientParams.getMaxGTLength()).addAllParquetColumns(sparkDriverClientParams.getParquetColumns()).//
                setUseII(true).setRealizationType(sparkDriverClientParams.getRealizationType()).//
                build();

        try {
            return blockingStub.submitJob(request);
        } catch (Exception e) {
            Status status = Status.fromThrowable(e);
            logger.error("error description:" + status.getDescription());
            throw new RuntimeException("RPC failed", e);
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

}
