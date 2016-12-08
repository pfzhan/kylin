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

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.ConfServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkConfRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;
import kap.google.protobuf.ByteString;

public class SparkDriverClient {
    private static final Logger logger = LoggerFactory.getLogger(SparkDriverClient.class);

    private static ManagedChannel channel;

    public SparkDriverClient(KapConfig kapConfig) {

        String host = kapConfig.getSparkClientHost();
        int port = kapConfig.getSparkClientPort();
        int maxMessageSize = kapConfig.getGrpcMaxResponseSize();

        if (channel == null) {
            synchronized (SparkDriverClient.class) {
                logger.info("SparkDriverClient host {}, port {}", host, port);
                channel = NettyChannelBuilder.forAddress(host, port).usePlaintext(true).maxMessageSize(maxMessageSize).build();

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                        System.err.println("*** shutting down gRPC channle since JVM is shutting down");
                        if (channel != null) {
                            try {
                                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                System.err.println("error when shutting down channel" + e.getMessage());
                            }
                        }
                        System.err.println("*** client shut down");
                    }
                });
            }
        }

        logger.info("Finish creating channel. ");
    }

    public SparkJobResponse submit(byte[] gtScanReq, SparkDriverClientParams sparkDriverClientParams) {
        SparkJobRequest request = SparkJobRequest.newBuilder().setGtScanRequest(ByteString.copyFrom(gtScanReq)).//
                setKylinProperties(sparkDriverClientParams.getKylinProperties()).setRealizationId(sparkDriverClientParams.getRealizationId()).//
                setSegmentId(sparkDriverClientParams.getSegmentId()).setDataFolderName(sparkDriverClientParams.getCuboidId()).//
                setMaxRecordLength(sparkDriverClientParams.getMaxGTLength()).addAllParquetColumns(sparkDriverClientParams.getParquetColumns()).//
                setUseII(sparkDriverClientParams.isUseII()).setRealizationType(sparkDriverClientParams.getRealizationType()).//
                setQueryId(sparkDriverClientParams.getQueryId()).//
                build();

        try {
            return JobServiceGrpc.newBlockingStub(channel).submitJob(request);
        } catch (Exception e) {
            Status status = Status.fromThrowable(e);
            logger.error("error description:" + status.getDescription());
            throw new RuntimeException("RPC failed", e);
        }
    }

    public String getSparkConf(String confName) {
        SparkConfRequest request = SparkConfRequest.newBuilder().setName(confName).build();
        return ConfServiceGrpc.newBlockingStub(channel).getConf(request).getValue();
    }
}
