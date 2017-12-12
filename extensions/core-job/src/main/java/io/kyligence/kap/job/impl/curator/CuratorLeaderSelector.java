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

package io.kyligence.kap.job.impl.curator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.LeaderSelector;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.Participant;

public class CuratorLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderSelector.class);
    private final String name;
    private final LeaderSelector leaderSelector;
    private JobEngineConfig jobEngineConfig;
    private DefaultScheduler defaultScheduler = null;

    CuratorLeaderSelector(CuratorFramework client, String path, String name, JobEngineConfig jobEngineConfig) {
        this.name = name;
        this.leaderSelector = new LeaderSelector(client, path, this);
        this.leaderSelector.setId(name);
        this.leaderSelector.autoRequeue();
        this.jobEngineConfig = jobEngineConfig;
        this.defaultScheduler = DefaultScheduler.getInstance();
    }

    public Participant getLeader() {
        try {
            return leaderSelector.getLeader();
        } catch (Exception e) {
            logger.error("Can not get leader.", e);
        }
        return new Participant("", false);
    }

    public List<Participant> getParticipants() {
        List<Participant> r = new ArrayList<>();
        try {
            r.addAll(leaderSelector.getParticipants());
        } catch (Exception e) {
            logger.error("Can not get participants.", e);
        }
        return r;
    }

    public boolean hasDefaultSchedulerStarted() {
        return defaultScheduler.hasStarted();
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    public boolean hasLeadership() throws IOException {
        return leaderSelector.hasLeadership();
    }

    @Override
    public void close() throws IOException {
        try {
            leaderSelector.close();
        } catch (IllegalStateException e) {
            if (e.getMessage().equals("Already closed or has not been started")) {
                logger.warn("LeaderSelector already closed or has not been started");
            } else {
                throw e;
            }
        }
        logger.info(name + " is stopped");
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        logger.info(name + " is the leader for job engine now.");
        defaultScheduler.init(jobEngineConfig, new MockJobLock());

        try {
            while (true) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
            }
        } catch (InterruptedException var6) {
            logger.error(this.name + " was interrupted.", var6);
            Thread.currentThread().interrupt();
        } finally {
            logger.warn(this.name + " relinquishing leadership.");
            if (defaultScheduler != null)
                defaultScheduler.shutdown();
        }
    }
}