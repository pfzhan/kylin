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
package io.kyligence.kap.newten.clickhouse;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.testcontainers.Testcontainers;

import java.net.URI;
import java.util.Locale;

@Slf4j
public class EmbeddedHttpServer {
    private final Server server;
    public final URI serverUri;
    public final URI uriAccessedByDocker;

    public EmbeddedHttpServer(Server server, URI serverUri, URI uriAccessedByDocker) {
        this.server = server;
        this.serverUri = serverUri;
        this.uriAccessedByDocker = uriAccessedByDocker;
    }

    public static EmbeddedHttpServer startServer(String workingDir, int port) throws Exception {
        log.debug("start http server on port: {}", port);
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);

        ContextHandler contextHandler = new ContextHandler();
        ResourceHandler contentResourceHandler = new ResourceHandler();
        contentResourceHandler.setDirectoriesListed(true);
        contentResourceHandler.setResourceBase(workingDir);
        contextHandler.setContextPath("/");
        contextHandler.setHandler(contentResourceHandler);
        server.setHandler(contextHandler);
        server.start();
        int listenedPort = connector.getLocalPort();
        String host = connector.getHost();
        if (host == null) {
            host = "localhost";
        }
        URI serverUri = new URI(String.format(Locale.ROOT, "http://%s:%d", host, listenedPort));
        Testcontainers.exposeHostPorts(listenedPort);
        URI uriAccessedByDocker = new URI(String.format(Locale.ROOT, "http://host.testcontainers.internal:%d", listenedPort));
        return new EmbeddedHttpServer(server, serverUri, uriAccessedByDocker);
    }

    public void stopServer() throws Exception {
        if (!server.isStopped()) {
            server.stop();
            server.join();
        }
    }
}
