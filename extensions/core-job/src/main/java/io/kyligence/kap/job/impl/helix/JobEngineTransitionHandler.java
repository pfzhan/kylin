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

package io.kyligence.kap.job.impl.helix;

import java.util.concurrent.ConcurrentMap;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 */
@StateModelInfo(states = { "LEADER", "STANDBY", "OFFLINE" }, initialState = "OFFLINE")
public class JobEngineTransitionHandler extends TransitionHandler {
    private static final Logger logger = LoggerFactory.getLogger(JobEngineTransitionHandler.class);
    private final KylinConfig kylinConfig;

    private static ConcurrentMap<KylinConfig, JobEngineTransitionHandler> instanceMaps = Maps.newConcurrentMap();

    private JobEngineTransitionHandler(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public static JobEngineTransitionHandler getInstance(KylinConfig kylinConfig) {
        Preconditions.checkNotNull(kylinConfig);
        instanceMaps.putIfAbsent(kylinConfig, new JobEngineTransitionHandler(kylinConfig));
        return instanceMaps.get(kylinConfig);
    }

    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
        logger.info("JobEngineStateModel.onBecomeLeaderFromStandby()");
        try {
            DefaultScheduler scheduler = DefaultScheduler.createInstance();
            this.kylinConfig.setProperty("kylin.server.mode", "all");
            scheduler.init(new JobEngineConfig(this.kylinConfig), new MockJobLock());
            while (!scheduler.hasStarted()) {
                logger.error("scheduler has not been started");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            logger.error("error start DefaultScheduler", e);
            throw new RuntimeException(e);
        }
    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
        logger.info("JobEngineStateModel.onBecomeStandbyFromLeader()");
        this.kylinConfig.setProperty("kylin.server.mode", "query");
        DefaultScheduler.destroyInstance();

    }

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
        logger.info("JobEngineStateModel.onBecomeStandbyFromOffline()");

    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
        logger.info("JobEngineStateModel.onBecomeOfflineFromStandby()");

    }
}
