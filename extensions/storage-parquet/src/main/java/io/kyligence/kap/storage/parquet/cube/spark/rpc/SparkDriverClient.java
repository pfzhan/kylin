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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.ConfServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
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
                logger.info("Finish creating channel");
            }
        }

    }

    public IStorageVisitResponseStreamer submit(byte[] gtScanReq, SparkDriverClientParams sparkDriverClientParams) {
        final long startTime = System.currentTimeMillis();
        final SparkJobProtos.SparkJobRequestPayload payload = SparkJobProtos.SparkJobRequestPayload.newBuilder().setGtScanRequest(ByteString.copyFrom(gtScanReq)).//
                setKylinProperties(sparkDriverClientParams.getKylinProperties()).setRealizationId(sparkDriverClientParams.getRealizationId()).//
                setSegmentId(sparkDriverClientParams.getSegmentId()).setDataFolderName(sparkDriverClientParams.getCuboidId()).//
                setMaxRecordLength(sparkDriverClientParams.getMaxGTLength()).addAllParquetColumns(sparkDriverClientParams.getParquetColumns()).//
                setUseII(sparkDriverClientParams.isUseII()).setRealizationType(sparkDriverClientParams.getRealizationType()).//
                setQueryId(sparkDriverClientParams.getQueryId()).//
                build();
        final SparkJobRequest initialRequest = SparkJobRequest.newBuilder().setPayload(payload).build();
        final JobServiceGrpc.JobServiceStub asyncStub = JobServiceGrpc.newStub(channel);

        final List<SparkJobResponse> responses = new LinkedList<>();
        final AtomicBoolean serverSideCompleted = new AtomicBoolean(false);
        final AtomicBoolean serverSideError = new AtomicBoolean(false);
        final Semaphore semaphore = new Semaphore(0);

        final StreamObserver<SparkJobRequest> requestObserver = asyncStub.submitJob(new StreamObserver<SparkJobResponse>() {
            @Override
            public void onNext(SparkJobResponse sparkJobResponse) {
                responses.add(sparkJobResponse);
                semaphore.release();
            }

            @Override
            public void onError(Throwable throwable) {
                Status status = Status.fromThrowable(throwable);
                logger.error("grpc client side receive error: " + status);
                serverSideError.set(true);
                semaphore.release();
            }

            @Override
            public void onCompleted() {
                logger.info("grpc client side receive complete.");
                serverSideCompleted.set(true);
                semaphore.release();
            }
        });

        //start
        requestObserver.onNext(initialRequest);

        return new KyStorageVisitResponseStreamer(semaphore, serverSideCompleted, serverSideError, responses, requestObserver, startTime);
    }

    public String getSparkConf(String confName) {
        SparkConfRequest request = SparkConfRequest.newBuilder().setName(confName).build();
        return ConfServiceGrpc.newBlockingStub(channel).getConf(request).getValue();
    }

    private static class KyStorageVisitResponseStreamer implements IStorageVisitResponseStreamer {

        private final Semaphore semaphore;
        private final AtomicBoolean serverSideCompleted;
        private final AtomicBoolean serverSideError;
        private final List<SparkJobResponse> responses;
        private final StreamObserver<SparkJobRequest> requestObserver;
        private final long startTime;
        SparkJobRequest subsequentRequest;
        private boolean fetched;

        public KyStorageVisitResponseStreamer(Semaphore semaphore, AtomicBoolean serverSideCompleted, AtomicBoolean serverSideError, List<SparkJobResponse> responses, StreamObserver<SparkJobRequest> requestObserver, long startTime) {
            this.semaphore = semaphore;
            this.serverSideCompleted = serverSideCompleted;
            this.serverSideError = serverSideError;
            this.responses = responses;
            this.requestObserver = requestObserver;
            this.startTime = startTime;
            subsequentRequest = null;
            fetched = false;
        }

        @Override
        public boolean hasNext() {
            if (fetched)
                return true;

            try {
                semaphore.acquire();

                if (serverSideCompleted.get()) {
                    return false;
                }
                if (serverSideError.get()) {
                    throw new RuntimeException("Failed to visit KyStorage! check logs/spark-driver.log for more details");
                }

                if (responses.size() != 1) {
                    throw new IllegalStateException("the number of responses in queue is abnormal: " + responses.size());
                }

                fetched = true;
                return true;

            } catch (InterruptedException e) {
                requestObserver.onError(e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public SparkJobResponse next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            if (responses.size() != 1) {
                throw new IllegalStateException("the number of responses in queue is abnormal: " + responses.size());
            }
            SparkJobResponse jobResponse = responses.get(0);
            responses.clear();
            fetched = false;

            //prefetch for next
            if (subsequentRequest == null) {
                String streamIdentifier = jobResponse.getStreamIdentifier();
                subsequentRequest = SparkJobRequest.newBuilder().setStreamIdentifier(streamIdentifier).build();
            } else {
                if (!subsequentRequest.getStreamIdentifier().equals(jobResponse.getStreamIdentifier())) {
                    throw new IllegalStateException("streamIdentifier inconsistent");
                }
            }
            requestObserver.onNext(subsequentRequest);

            return jobResponse;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<byte[]> asByteArrayIterator() {
            final MutableInt responseCount = new MutableInt(0);
            Iterator<byte[]> transform = Iterators.transform(//
                    Iterators.concat(//
                            Iterators.transform(this, new Function<SparkJobResponse, Iterator<SparkJobResponse.ShardBlob>>() {
                                @Override
                                public Iterator<SparkJobResponse.ShardBlob> apply(@Nullable SparkJobResponse sparkJobResponse) {
                                    logger.info("Time for the {}th gRPC response message of query {} from spark instance {} visit is {}", //
                                            responseCount, QueryContext.getQueryId(), sparkJobResponse.getSparkInstanceIdentifier(), (System.currentTimeMillis() - startTime));
                                    responseCount.increment();
                                    return sparkJobResponse.getShardBlobsList().iterator();
                                }
                            })),
                    new Function<SparkJobResponse.ShardBlob, byte[]>() {
                        @Override
                        public byte[] apply(@Nullable SparkJobResponse.ShardBlob shardBlob) {

                            byte[] bytes = shardBlob.getBlob().toByteArray();
                            logger.info("size of partition: {}", bytes.length);

                            //only for debug/profile purpose
                            if (BackdoorToggles.getPartitionDumpDir() != null) {
                                logger.info("debugging: Dumping partitions");
                                dumpPartitions(bytes);
                            }

                            return bytes;
                        }
                    });
            return transform;
        }

        private void dumpPartitions(byte[] bytes) {
            File dir = new File(BackdoorToggles.getPartitionDumpDir());
            if (!dir.exists()) {
                dir.mkdirs();
            }

            if (dir.exists() && dir.isDirectory()) {
                int numFiles = dir.listFiles().length;
                File f = new File(dir, String.valueOf(numFiles));
                try (FileOutputStream fileOutputStream = new FileOutputStream(f, false)) {
                    IOUtils.write(bytes, fileOutputStream);
                } catch (Exception e) {
                    logger.error("error", e);
                }
            } else {
                logger.error("BackdoorToggles.getPartitionDumpDir() not valid dir for dumping");
            }

        }

        @Override
        public void close() throws IOException {
            logger.info("IStorageVisitResponseStreamer is finishing grpc session");
            requestObserver.onCompleted();
        }
    }
}
