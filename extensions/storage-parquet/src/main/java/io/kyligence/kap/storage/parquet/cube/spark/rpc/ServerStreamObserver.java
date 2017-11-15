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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.api.java.JavaRDD;
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
import io.kyligence.kap.storage.parquet.cube.spark.rpc.StorageVisitState.ResultPackStatus;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.StorageVisitState.TransferPack;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;
import io.kyligence.kap.storage.parquet.protocol.shaded.com.google.protobuf.ByteString;

public class ServerStreamObserver implements StreamObserver<SparkJobProtos.SparkJobRequest> {

    public static final Logger logger = LoggerFactory.getLogger(ServerStreamObserver.class);
    private final static ExecutorService asyncWorkers = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("kystorage-async-workers-" + t.getId());
            return t;
        }
    });

    private final static Cache<String, StorageVisitState> storageVisitStates = CacheBuilder.newBuilder()
            .maximumSize(10000).expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
                    new RemovalListener<String, StorageVisitState>() {
                        @Override
                        public void onRemoval(RemovalNotification<String, StorageVisitState> notification) {
                            ServerStreamObserver.logger.info(
                                    "StorageVisitState with streamIdentifier {} createTime {}(GMT) is removed due to {} ",
                                    notification.getKey(), //
                                    DateFormat.formatToTimeStr(checkNotNull(notification.getValue()).getCreateTime()),
                                    notification.getCause());
                        }
                    })
            .build();

    private final String sparkInstanceIdentifier = System.getProperty("kap.spark.identifier") == null ? ""
            : System.getProperty("kap.spark.identifier");
    private final String streamIdentifier = UUID.randomUUID().toString();// it will stay during the stream session
    private final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver;
    private final JavaSparkContext sc;
    private final AtomicBoolean lastMessageSent = new AtomicBoolean(false);

    public ServerStreamObserver(final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver,
            JavaSparkContext sc) {
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
            onFail(responseObserver, streamIdentifier,
                    "StorageVisitState for stream " + streamIdentifier + " is missing!");
        } else {

            TraceScope scope = null;
            if (state.getTraceInfo() != null) {
                scope = Trace.startSpan("subsequent grpc call", state.getTraceInfo());
            }

            getRDDPartitionData(state);

            if (scope != null)
                scope.close();
        }
    }

    private void dealWithNewVisit(final SparkJobProtos.SparkJobRequest sparkJobRequest) {
        //start a new visit 
        if (sparkJobRequest.hasStreamIdentifier()) {
            throw new IllegalStateException("payload and streamIdentifier cannot coexist in same request");
        }

        final StorageVisitState state = new StorageVisitState();
        if (sparkJobRequest.getPayload().hasTraceInfo()) {
            SparkJobProtos.TraceInfo traceInfo = sparkJobRequest.getPayload().getTraceInfo();
            long traceId = traceInfo.getTraceId();
            long spanId = traceInfo.getSpanId();
            TraceInfo tinfo = new TraceInfo(traceId, spanId);
            state.setTraceInfo(tinfo);
        }

        TraceScope scope = null;
        if (state.getTraceInfo() != null) {
            scope = Trace.startSpan("first grpc call", state.getTraceInfo());
        }

        storageVisitStates.put(streamIdentifier, state);

        logger.info("StorageVisitState for queryID {} scanReqId {} : streamIdentifier {}",
                sparkJobRequest.getPayload().getQueryId(), sparkJobRequest.getPayload().getGtScanRequestId(),
                streamIdentifier);

        asyncWorkers.submit(Trace.wrap("async spark thread", new Runnable() {
            @Override
            public void run() {
                logger.info("Current stream identifier is {} ", streamIdentifier);
                doStorageVisit(sparkJobRequest.getPayload());
            }
        }));

        getRDDPartitionData(state);

        if (scope != null)
            scope.close();
    }

    private void doStorageVisit(SparkJobProtos.SparkJobRequestPayload request) {
        final StorageVisitState state = storageVisitStates.getIfPresent(streamIdentifier);
        if (state == null) {
            logger.error("StorageVisitState does not exist!");
            //throwing is meaningless, it's in a Executor
            return;
        }

        SynchronousQueue<TransferPack> synchronousQueue = null;
        try {
            long startTime = System.currentTimeMillis();
            synchronousQueue = state.getSynchronousQueue();
            RDDPartitionResult current = null;

            SparkParquetVisit visit = new SparkParquetVisit(sc, request, streamIdentifier);

            while (true) {
                if (!visit.moreRDDExists()) {
                    return;
                }

                Pair<Iterator<RDDPartitionResult>, JavaRDD<RDDPartitionResult>> pair = visit.executeTask();
                Iterator<RDDPartitionResult> resultPartitions = pair.getFirst();
                JavaRDD<RDDPartitionResult> baseRDD = pair.getSecond();

                logger.info(
                        "Time for spark parquet visit execution (may not include result fetch if it's large result) is: "
                                + (System.currentTimeMillis() - startTime));
                boolean moreRDDStillExists = visit.moreRDDExists();

                while (true) {
                    if (current == null) {
                        if (!resultPartitions.hasNext()) {
                            break;
                        }
                        current = resultPartitions.next();
                    }

                    StorageVisitState temp = storageVisitStates.getIfPresent(streamIdentifier);
                    if (temp == null) {
                        logger.info(
                                "Skip offering resting RDDPartitionData because current session with streamIdentifier {} does not seem active anymore",
                                streamIdentifier);
                        return;
                    }

                    TransferPack transferPack = (moreRDDStillExists || resultPartitions.hasNext())
                            ? TransferPack.createNormalPack(current) : TransferPack.createLastPack(current);

                    Trace.addTimelineAnnotation("one result pack pending transfer");
                    boolean success = synchronousQueue.offer(transferPack, 1, TimeUnit.MINUTES);
                    if (success) {
                        Trace.addTimelineAnnotation("one result pack transferred");
                        current = null;
                    } else {
                        logger.info("storage visit producer for streamIdentifier {} continue to try...",
                                streamIdentifier);
                    }

                    if (state.isUserCanceled()) {
                        logger.info("Skip offering rest RDDPartitionData because client cancelled");
                        return;
                    }
                }

                baseRDD.unpersist();
            }
        } catch (Exception e) {
            logger.warn("Error in doStorageVisit, notifying the root cause");
            if (synchronousQueue != null) {
                try {
                    boolean offer = synchronousQueue.offer(TransferPack.createErrorPack(e), 1, TimeUnit.MINUTES);
                    if (!offer) {
                        logger.error("Failed to transfer the error pack");
                    }
                } catch (InterruptedException e1) {
                    logger.error("InterruptedException when transfer the error pack", e1);
                }
            }
        }
    }

    private void getRDDPartitionData(StorageVisitState state) {
        try {
            if (lastMessageSent.get()) {
                responseObserver.onCompleted();
                logger.info("Sent back last empty message for stream {}, total number of non-empty messages: {}",
                        streamIdentifier, state.incAndGetMessageNum());
                return;
            }

            //loop check results
            TransferPack transferPack = null;
            int retry = 0;

            while (true) {

                if (++retry > 60) {
                    throw new GetRDDPartitionFailureException("Exceed max retry time");
                }

                if (retry > 1) {
                    logger.info("storage visit consumer for streamIdentifier {} trying {}th time...", streamIdentifier,
                            retry);
                }

                try {
                    transferPack = state.getSynchronousQueue().poll(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    //seldom happens
                    throw new GetRDDPartitionFailureException(e);
                }

                if (transferPack != null) {
                    if (transferPack.status == ResultPackStatus.ERROR) {
                        throw new GetRDDPartitionFailureException(transferPack.visitThreadFailCause);
                    } else {
                        RDDPartitionResult resultPartition = transferPack.rddPartitionResult;
                        SparkJobProtos.SparkJobResponse.Builder builder = SparkJobProtos.SparkJobResponse.newBuilder()
                                .setSparkInstanceIdentifier(sparkInstanceIdentifier)
                                .setStreamIdentifier(streamIdentifier).addPartitionResponse(//
                                        SparkJobProtos.SparkJobResponse.PartitionResponse.newBuilder().//
                                                setBlob(ByteString.copyFrom(resultPartition.getData())).//
                                                setScannedRows(resultPartition.getScannedRows()).//
                                                setScannedBytes(resultPartition.getScannedBytes()).//
                                                setReturnedRows(resultPartition.getReturnedRows()).//
                                                setHostname(resultPartition.getHostname()).//
                                                setTotalDuration(resultPartition.getTotalDuration()).//
                                                setStartLatency(resultPartition.getStartLatency()).//
                                                build());
                        responseObserver.onNext(builder.build());

                        if (transferPack.status == ResultPackStatus.LAST) {
                            lastMessageSent.set(true);
                        }
                        logger.info("Sent back {}th message for stream {}", state.incAndGetMessageNum(),
                                streamIdentifier);
                        break;
                    }
                }
            }

        } catch (GetRDDPartitionFailureException e) {
            logger.error("Details of the GetRDDPartitionFailureException:", e);
            onFail(responseObserver, streamIdentifier, e);
        }
    }

    private void onFail(final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver, String streamIdentifier,
            Throwable cause) {
        Status status = null;
        if (ExceptionUtils.getRootCause(cause) instanceof ResourceLimitExceededException) {
            status = Status.DEADLINE_EXCEEDED;
        } else {
            status = Status.INTERNAL;
        }
        StatusRuntimeException statusRuntimeException = status
                .withDescription("KyStorage failure due to: " + cause.getMessage()).withCause(cause)
                .asRuntimeException();
        responseObserver.onError(statusRuntimeException);

        storageVisitStates.invalidate(streamIdentifier);//clean the state explicitly
    }

    private void onFail(final StreamObserver<SparkJobProtos.SparkJobResponse> responseObserver, String streamIdentifier,
            String message) {
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
        storageVisitStates.invalidate(streamIdentifier);
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
