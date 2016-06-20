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

package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;

public class JobServiceClient {
    private static final Logger logger = LoggerFactory.getLogger(JobServiceClient.class);

    private final ManagedChannel channel;
    private final JobServiceGrpc.JobServiceBlockingStub blockingStub;

    public JobServiceClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
        blockingStub = JobServiceGrpc.newBlockingStub(channel);

    }

    public void submit(String name) {
        logger.info("Will try to greet " + name + " ...");
        SparkJobRequest request = SparkJobRequest.newBuilder().setRequest(ByteString.EMPTY).build();
        SparkJobResponse response;
        try {
            response = blockingStub.submitJob(request);
        } catch (StatusRuntimeException e) {
            logger.warn("rpc failed: {}", e.getStatus());
            return;
        }
        logger.info("Submitting: " + new String(response.getResponse().toByteArray()));
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        JobServiceClient client = new JobServiceClient("localhost", 50051);
        try {
            /* Access a service running on the local machine on port 50051 */
            String user = "world";
            if (args.length > 0) {
                user = args[0]; /* Use the arg as the name to greet if provided */
            }
            client.submit(user);
        } finally {
            client.shutdown();
        }
    }
}
