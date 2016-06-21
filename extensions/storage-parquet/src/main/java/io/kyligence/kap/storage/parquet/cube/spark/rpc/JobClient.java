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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkJobResponse;

public class JobClient {
    private static final Logger logger = LoggerFactory.getLogger(JobClient.class);

    private final ManagedChannel channel;
    private final JobServiceGrpc.JobServiceBlockingStub blockingStub;

    public JobClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
        blockingStub = JobServiceGrpc.newBlockingStub(channel);

    }

    public void submit(byte[] requestBlob) {
        SparkJobRequest request = SparkJobRequest.newBuilder().setRequest(ByteString.copyFrom(requestBlob)).build();
        SparkJobResponse response;
        try {
            response = blockingStub.submitJob(request);
        } catch (StatusRuntimeException e) {
            logger.warn("rpc failed: {}", e.getStatus());
            return;
        }
        logger.info("result from the server " + Bytes.toLong(response.getResponse().toByteArray()));
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        JobClient client = new JobClient("localhost", 50051);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            for (;;) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                int inputValue = Integer.valueOf(line);
                client.submit(Bytes.toBytes(inputValue));
            }
        } finally {
            client.shutdown();
        }
    }
}
