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

package io.kyligence.kap.metadata.epoch;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.AuditLogReplayWorker;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import lombok.Synchronized;

/**
 */
public class EpochOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(EpochOrchestrator.class);

    private final EpochManager epochMgr;

    private ScheduledExecutorService checkerPool;

    private volatile boolean isCheckerRunning = true;

    private static final String OWNER_IDENTITY;

    static {
        OWNER_IDENTITY = AddressUtil.getLocalInstance() + "|" + System.currentTimeMillis();
    }

    public static String getOwnerIdentity() {
        return OWNER_IDENTITY;
    }

    public EpochOrchestrator(KylinConfig kylinConfig) {
        epochMgr = EpochManager.getInstance();
        String serverMode = kylinConfig.getServerMode();
        if (!kylinConfig.isJobNode() && !kylinConfig.isMetadataNode()) {
            logger.info("server mode: {},  no need to run EventOrchestrator", serverMode);
            return;
        }

        long pollSecond = kylinConfig.getEpochCheckerIntervalSecond();
        logger.info("Try to update epoch every {} seconds", pollSecond);
        logger.info("renew executor work size is :{}", kylinConfig.getRenewEpochWorkerPoolSize());

        checkerPool = Executors.newScheduledThreadPool(2, new NamedThreadFactory("EpochChecker"));
        checkerPool.scheduleWithFixedDelay(new EpochChecker(), 1, pollSecond, TimeUnit.SECONDS);
        checkerPool.scheduleAtFixedRate(new EpochRenewer(), pollSecond, pollSecond, TimeUnit.SECONDS);

        EventBusFactory.getInstance().register(new ReloadMetadataListener(), true);
    }

    public void shutdown() {
        logger.info("Shutting down EpochOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.shutdownGracefully(checkerPool, 60);
    }

    public void forceShutdown() {
        logger.info("Shutting down EpochOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.forceShutdown(checkerPool);
    }

    @Synchronized
    void updateCheckerStatus(boolean isRunning) {
        logger.info("Change epoch checker status from {} to {}", isCheckerRunning, isRunning);
        isCheckerRunning = isRunning;
    }

    class EpochChecker implements Runnable {

        @Override
        public synchronized void run() {
            try {
                if (!isCheckerRunning) {
                    return;
                }
                epochMgr.getEpochUpdateManager().tryUpdateAllEpochs();
            } catch (Exception e) {
                logger.error("Failed to update epochs");
            }
        }
    }

    class EpochRenewer implements Runnable {

        private final AtomicBoolean raceCheck = new AtomicBoolean(false);

        @Override
        public void run() {
            try {
                if (!isCheckerRunning || !raceCheck.compareAndSet(false, true)) {
                    return;
                }
                epochMgr.getEpochUpdateManager().tryRenewOwnedEpochs();
            } catch (Exception e) {
                logger.error("Failed to renew epochs", e);
            } finally {
                raceCheck.compareAndSet(true, false);
            }
        }
    }

    class ReloadMetadataListener {

        @Subscribe
        public void onStart(AuditLogReplayWorker.StartReloadEvent start) {
            updateCheckerStatus(false);
            epochMgr.releaseOwnedEpochs();
        }

        @Subscribe
        public void onEnd(AuditLogReplayWorker.EndReloadEvent end) {
            updateCheckerStatus(true);
            epochMgr.updateAllEpochs();
        }
    }

}
