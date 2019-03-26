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
package io.kyligence.kap.rest.config;

import org.apache.kylin.common.KylinConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.cluster.NodeCandidate;
import io.kyligence.kap.common.metric.InfluxDBWriter;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.common.persistence.transaction.MessageSynchronization;
import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import io.kyligence.kap.rest.config.initialize.AppInitializedEvent;
import io.kyligence.kap.rest.config.initialize.BootstrapCommand;
import io.kyligence.kap.rest.config.initialize.FavoriteQueryUpdateListener;
import io.kyligence.kap.rest.config.initialize.ProjectDropListener;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AppInitializer {

    @Autowired
    TaskScheduler taskScheduler;

    @EventListener(ContextRefreshedEvent.class)
    public void init(ContextRefreshedEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.metadata.url.identifier", kylinConfig.getMetadataUrlPrefix());

        val candidate = new NodeCandidate(kylinConfig.getNodeId());
        val leaderInitiator = LeaderInitiator.getInstance(kylinConfig);
        leaderInitiator.start(candidate);

        if (leaderInitiator.isLeader()) {
            taskScheduler.scheduleWithFixedDelay(new BootstrapCommand(), 10000);
        } else {
            val messageQueue = MessageQueue.getInstance(kylinConfig);
            if (messageQueue != null) {
                val replayer = MessageSynchronization.getInstance(kylinConfig);
                messageQueue.startConsumer(replayer::replay);
            }
        }

        // init influxDB writer and create DB
        try {
            InfluxDBWriter.getInstance();
        } catch (Exception ex) {
            log.error("InfluxDB writer has not initialized");
        }

        EventListenerRegistry.getInstance(kylinConfig).register(new FavoriteQueryUpdateListener(), "fq");
        EventListenerRegistry.getInstance(kylinConfig).register(new ProjectDropListener(), "pd");
        event.getApplicationContext().publishEvent(new AppInitializedEvent(event.getApplicationContext()));
    }

}
