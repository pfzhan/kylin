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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.htrace.HtraceInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

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
        HtraceInit.init();
        server = NettyServerBuilder.forPort(port).maxMessageSize(64 * 1024 * 1024).addService(new SparkAppClientService()).addService(new SparkConfService()).build().start();
        logger.info("Server started, listening on " + port + " with spark instance identifier: " + System.getProperty("kap.spark.identifier"));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                if (server != null) {
                    try {
                        server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        System.err.println("error when shutting down channel" + e.getMessage());
                    }
                }
                System.err.println("*** server shut down");
            }
        });
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
