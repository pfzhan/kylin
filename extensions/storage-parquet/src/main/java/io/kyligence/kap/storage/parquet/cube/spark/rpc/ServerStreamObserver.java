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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.DateFormat;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import kap.google.protobuf.ByteString;

public class ServerStreamObserver implements StreamObserver<SparkJobProtos.SparkJobRequest> {

    public static final Logger logger = LoggerFactory.getLogger(ServerStreamObserver.class);
    private final static ExecutorService pool = Executors.newCachedThreadPool();
    private final static Cache<String, StorageVisitState> storageVisitStates = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
            new RemovalListener<String, StorageVisitState>() {
                @Override
                public void onRemoval(RemovalNotification<String, StorageVisitState> notification) {
                    SparkAppClientService.logger.info("StorageVisitState with streamIdentifier {} createTime {}(GMT) is removed due to {} ", notification.getKey(), //
                            DateFormat.formatToTimeStr(checkNotNull(notification.getValue()).getCreateTime()), notification.getCause());
                }
            }).build();

    private final String sparkInstanceIdentifier = System.getProperty("kap.spark.identifier") == null ? "" : System.getProperty("kap.spark.identifier");
    private final String streamIdentifier = UUID.randomUUID().toString();// it will stay during the stream session
    private final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver;
    private final JavaSparkContext sc;

    public ServerStreamObserver(final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver, JavaSparkContext sc) {
        this.responseObserver = responseObserver;
        this.sc = sc;
    }

    @Override
    public void onNext(final SparkJobProtos.SparkJobRequest sparkJobRequest) {
        if (sparkJobRequest.hasPayload()) {
            dealWithNewVisit(sparkJobRequest);
        } else if (sparkJobRequest.hasStreamIdentifier()) { //continue last visit 
            continueLastVisit(sparkJobRequest);
        } else {
            throw new IllegalStateException("Either provide payload or streamIdentifier");
        }
    }

    private void continueLastVisit(SparkJobProtos.SparkJobRequest sparkJobRequest) {
        if (!streamIdentifier.equals(sparkJobRequest.getStreamIdentifier())) {
            throw new IllegalStateException("streamIdentifier inconsistent!");
        }

        StorageVisitState state = storageVisitStates.getIfPresent(streamIdentifier);
        if (state == null) {
            onFail(responseObserver, streamIdentifier, "StorageVisitState for stream " + streamIdentifier + " is missing!");
        } else {
            getRDDPartitionData(state);
        }
    }

    private void dealWithNewVisit(final SparkJobProtos.SparkJobRequest sparkJobRequest) {
        //start a new visit 
        if (sparkJobRequest.hasStreamIdentifier()) {
            throw new IllegalStateException("payload and streamIdentifier cannot coexist in same request");
        }

        final StorageVisitState state = new StorageVisitState();
        storageVisitStates.put(streamIdentifier, state);
        logger.info("Create a new StorageVisitState for queryID {} with streamIdentifier {}", sparkJobRequest.getPayload().getQueryId(), streamIdentifier);

        pool.submit(new Runnable() {
            @Override
            public void run() {
                doStorageVisit(sparkJobRequest.getPayload());
            }
        });

        getRDDPartitionData(state);

    }

    private void doStorageVisit(SparkJobProtos.SparkJobRequestPayload request) {

        final StorageVisitState state = storageVisitStates.getIfPresent(streamIdentifier);
        if (state == null) {
            logger.error("StorageVisitState does not exist!");
            //throwing is meaningless, it's in a Executor
            return;
        }

        try {
            long startTime = System.currentTimeMillis();

            SparkParquetVisit submit = new SparkParquetVisit(sc, request);
            Iterator<SparkParquetVisit.RDDPartitionData> resultPartitions = submit.executeTask();

            logger.info("Time for spark parquet visit execution (may not include result fetch if it's large result) is " + (System.currentTimeMillis() - startTime));

            SynchronousQueue<SparkParquetVisit.RDDPartitionData> synchronousQueue = state.getSynchronousQueue();
            SparkParquetVisit.RDDPartitionData current = null;

            boolean normalFinish = false;
            while (true) {
                if (current == null) {
                    if (!resultPartitions.hasNext()) {
                        normalFinish = true;
                        break;
                    }
                    current = resultPartitions.next();
                }

                StorageVisitState temp = storageVisitStates.getIfPresent(streamIdentifier);
                if (temp == null) {
                    logger.info("Skip offering rest RDDPartitionData because current session with streamIdentifier {} does not seem active anymore", streamIdentifier);
                    break;
                }

                if (state.isUserCanceled()) {
                    logger.info("Skip offering rest RDDPartitionData because client cancelled");
                    break;
                } else {
                    logger.info("storage visit producer for streamIdentifier {} continue to try...", streamIdentifier);
                }

                boolean success = synchronousQueue.offer(current, 1, TimeUnit.MINUTES);
                if (success) {
                    current = null;
                }
            }

            if (normalFinish) {
                state.setNoMore(true);
                logger.info("Storage visit thread has transferred all partitions");
            }

        } catch (Exception e) {
            state.setVisitThreadFailCause(e);
        }
    }

    private void getRDDPartitionData(StorageVisitState state) {
        try {
            //loop check results
            SparkParquetVisit.RDDPartitionData resultPartition = null;
            while (true) {
                if (state.isNoMore()) {
                    responseObserver.onCompleted();
                    break;
                }

                if (state.isVisitThreadFailed()) {
                    throw new GetRDDPartitionFailureException(state.getVisitThreadFailCause());
                } else {
                    logger.info("storage visit consumer for streamIdentifier {} continue to try...", streamIdentifier);
                }

                try {
                    resultPartition = state.getSynchronousQueue().poll(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    //seldom happens
                    throw new GetRDDPartitionFailureException(e);
                }

                if (resultPartition != null) {
                    SparkJobProtos.SparkJobResponse.Builder builder = SparkJobProtos.SparkJobResponse.newBuilder().setSparkInstanceIdentifier(sparkInstanceIdentifier).setStreamIdentifier(streamIdentifier)//
                            .addShardBlobs(SparkJobProtos.SparkJobResponse.ShardBlob.newBuilder().setBlob(ByteString.copyFrom(resultPartition.getData())).build());
                    responseObserver.onNext(builder.build());
                    break;
                }
            }

            logger.info("Finish sending back {}th message for stream {}", state.incAndGetMessageNum(), streamIdentifier);

        } catch (GetRDDPartitionFailureException e) {
            logger.error("Meet GetRDDPartitionFailureException", e);
            onFail(responseObserver, streamIdentifier, e);
        }
    }

    private void onFail(final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver, String streamIdentifier, Throwable cause) {
        StatusRuntimeException statusRuntimeException = Status.INTERNAL.withDescription("Storage visit failed due to: " + cause.getMessage()).withCause(cause).asRuntimeException();
        responseObserver.onError(statusRuntimeException);

        storageVisitStates.invalidate(streamIdentifier);//clean the state explicitly
    }

    private void onFail(final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver, String streamIdentifier, String message) {
        StatusRuntimeException statusRuntimeException = Status.INTERNAL.withDescription(message).asRuntimeException();
        responseObserver.onError(statusRuntimeException);

        storageVisitStates.invalidate(streamIdentifier);//clean the state explicitly
    }

    @Override
    public void onError(Throwable throwable) {
        Status status = Status.fromThrowable(throwable);
        logger.error("grpc server side receive error: " + status);
        setUserCancelledIfPossible();
    }

    @Override
    public void onCompleted() {
        logger.info("grpc server side receive complete.");
        setUserCancelledIfPossible();
        responseObserver.onCompleted();
    }

    private void setUserCancelledIfPossible() {
        StorageVisitState state = storageVisitStates.getIfPresent(streamIdentifier);
        if (state != null) {
            logger.info("Setting StorageVisitState.userCanceled to true");
            state.setUserCanceled(true);
        } else {
            logger.warn("Cannot find StorageVisitState with streamIdentifier {}", streamIdentifier);
        }
    }
}
