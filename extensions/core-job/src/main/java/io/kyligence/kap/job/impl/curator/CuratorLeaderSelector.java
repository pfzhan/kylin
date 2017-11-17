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

import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.LeaderSelector;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 */
public class CuratorLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderSelector.class);
    private final String name;
    private final LeaderSelector leaderSelector;
    private JobEngineConfig jobEngineConfig;
    private DefaultScheduler defaultScheduler = null;

    public CuratorLeaderSelector(CuratorFramework client, String path, String name, JobEngineConfig jobEngineConfig) {
        this.name = name;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
        this.jobEngineConfig = jobEngineConfig;
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
        logger.info(name + " is stopped");
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        logger.info(name + " is the leader for job engine now.");
        defaultScheduler = DefaultScheduler.createInstance();
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