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


package io.kyligence.kap.rest.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CheckUtil;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.rest.service.CacheService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

import org.apache.kylin.rest.broadcaster.BroadcasterReceiveServlet;

import java.util.HashSet;
import java.util.Set;

public class MultiNodeManagerTestBase extends LocalFileMetadataTestCase {
    private static Server server;
    protected static String PROJECT = "default";
    protected static String USER = "u1";
    protected static String TABLE = "t1";
    protected static Set<String> EMPTY_GROUP_SET = new HashSet<>();

    protected static KylinConfig configA;
    protected static KylinConfig configB;
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeManagerTestBase.class);

    @Before
    public void setup() throws Exception {
        staticCreateTestMetadata();
        System.clearProperty("kylin.server.cluster-servers");
        int port = CheckUtil.randomAvailablePort(40000, 50000);
        logger.info("Chosen port for CacheServiceTest is " + port);
        configA = KylinConfig.getInstanceFromEnv();
        configA.setProperty("kylin.server.cluster-servers", "localhost:" + port);
        configB = KylinConfig.createKylinConfig(configA);
        configB.setProperty("kylin.server.cluster-servers", "localhost:" + port);
        configB.setMetadataUrl("../examples/test_metadata");

        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        final CacheService serviceA = new CacheService() {
            @Override
            public KylinConfig getConfig() {
                return configA;
            }
        };
        final CacheService serviceB = new CacheService() {
            @Override
            public KylinConfig getConfig() {
                return configB;
            }
        };

        context.addServlet(new ServletHolder(new BroadcasterReceiveServlet(new BroadcasterReceiveServlet.BroadcasterHandler() {
            @Override
            public void handle(String entity, String cacheKey, String event) {
                Broadcaster.Event wipeEvent = Broadcaster.Event.getEvent(event);
                final String log = "wipe cache type: " + entity + " event:" + wipeEvent + " name:" + cacheKey;
                logger.info(log);
                try {
                    serviceA.notifyMetadataChange(entity, wipeEvent, cacheKey);
                    serviceB.notifyMetadataChange(entity, wipeEvent, cacheKey);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        })), "/");
        server.start();
    }

    @After
    public void after() throws Exception {
        server.stop();
        cleanAfterClass();
    }
}
