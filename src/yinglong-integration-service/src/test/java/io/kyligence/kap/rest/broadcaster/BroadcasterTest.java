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

package io.kyligence.kap.rest.broadcaster;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import io.kyligence.kap.rest.config.initialize.BroadcastListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
class BroadcasterTest {

    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testBroadcast() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            SpringContext springContext = context.getBean(SpringContext.class);
            ReflectionTestUtils.setField(springContext, "applicationContext", context);
            Broadcaster broadcaster = Broadcaster.getInstance(KylinConfig.getInstanceFromEnv(),
                    new BroadcastListener() {
                        @Override
                        public void handle(BroadcastEventReadyNotifier notifier) {
                            log.info("received notifier {}.", notifier);
                        }
                    });

            broadcaster.announce(new BroadcastEventReadyNotifier());

            await().atLeast(2, TimeUnit.SECONDS);

            ClusterManager clusterManager = (ClusterManager) ReflectionTestUtils.getField(broadcaster,
                    "clusterManager");
            Assertions.assertNotNull(clusterManager);
            Assertions.assertEquals(clusterManager.getClass(), DefaultClusterManager.class);
        }
    }

    @Configuration
    static class Config {
        @Bean
        @Primary
        public SpringContext springContext() {
            return Mockito.spy(new SpringContext());
        }

        @Bean
        public ClusterManager clusterManager() {
            return new DefaultClusterManager(7070);
        }
    }

}