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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.JobServiceGrpc;

public class SparkDriverServer {
    private static final Logger logger = LoggerFactory.getLogger(SparkDriverServer.class);

    private final static int defaultPort = 7071;
    private final int port;
    private Server server;

    public SparkDriverServer(int port) {
        this.port = port;
    }

    public SparkDriverServer() {
        /* The port on which the server should listen to */
        this.port = defaultPort;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port).addService(JobServiceGrpc.bindService(new SparkAppClientService())).build().start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                SparkDriverServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        SparkDriverServer server;
        if (args != null && args.length > 1) {
            System.out.println("Port is set to " + args[0]);
            server = new SparkDriverServer(Integer.valueOf(args[0]));
        } else {
            System.out.println("Port is set to default value " + defaultPort);
            server = new SparkDriverServer();
        }
        server.start();
        server.blockUntilShutdown();
    }

}
