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

package org.apache.kylin.job.impl.threadpool;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.metric.SystemInfoCollector;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.Getter;
import lombok.val;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.runners.FetcherRunner;
import org.apache.kylin.job.runners.JobCheckRunner;
import org.apache.kylin.job.runners.LicenseCapacityCheckRunner;
import org.apache.kylin.job.runners.QuotaStorageCheckRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class NDefaultScheduler implements Scheduler<AbstractExecutable> {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultScheduler.class);

    @Getter
    private String project;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    @Getter
    private ExecutableContext context;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean hasStarted = new AtomicBoolean(false);
    @Getter
    private JobEngineConfig jobEngineConfig;
    @Getter
    private static volatile Semaphore memoryRemaining = new Semaphore(Integer.MAX_VALUE);
    private long epochId = UnitOfWork.DEFAULT_EPOCH_ID;
    @Getter
    private AtomicInteger currentPrjUsingMemory = new AtomicInteger(0);
    private static final Map<String, NDefaultScheduler> INSTANCE_MAP = Maps.newConcurrentMap();

    public NDefaultScheduler() {
    }

    public NDefaultScheduler(String project) {
        Preconditions.checkNotNull(project);
        this.project = project;

        if (INSTANCE_MAP.containsKey(project))
            throw new IllegalStateException(
                    "DefaultScheduler for project " + project + " has been initiated. Use getInstance() instead.");

        logger.debug("New NDefaultScheduler created by project '{}': {}", project,
                System.identityHashCode(NDefaultScheduler.this));
    }

    public static synchronized NDefaultScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, NDefaultScheduler::new);
    }

    public void fetchJobsImmediately() {
        fetcherPool.schedule(new FetcherRunner(this, jobPool, fetcherPool), 1, TimeUnit.SECONDS);
    }

    public static List<NDefaultScheduler> listAllSchedulers() {
        return Lists.newArrayList(INSTANCE_MAP.values());
    }

    public static synchronized void destroyInstance() {

        for (Map.Entry<String, NDefaultScheduler> entry : INSTANCE_MAP.entrySet()) {
            entry.getValue().shutdown();
        }
        INSTANCE_MAP.clear();
    }

    public static synchronized void shutdownByProject(String project) {
        val instance = getInstanceByProject(project);
        if (instance != null) {
            INSTANCE_MAP.remove(project);
            instance.forceShutdown();
        }
    }

    public static synchronized NDefaultScheduler getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig) {

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv()) {
            this.epochId = EpochManager.getInstance(config).getEpochId(project);
        }

        String serverMode = jobEngineConfig.getServerMode();
        if (!("job".equalsIgnoreCase(serverMode) || "all".equalsIgnoreCase(serverMode))) {
            logger.info("server mode: {}, no need to run job scheduler", serverMode);
            return;
        }
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing Job Engine ....");

        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("FetchJobWorker(project:" + project + ")"));
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        if (config.getAutoSetConcurrentJob()) {
            corePoolSize = Integer.MAX_VALUE;
            val availableMemoryRate = config.getMaxLocalConsumptionRatio();
            synchronized (NDefaultScheduler.class) {
                if (Integer.MAX_VALUE == memoryRemaining.availablePermits()) {
                    memoryRemaining = new Semaphore(
                            (int) (SystemInfoCollector.getAvailableMemoryInfo() * availableMemoryRate));
                }
            }

            logger.info("Scheduler memory remaining: {}", memoryRemaining.availablePermits());
        }
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<>(), new NamedThreadFactory("RunJobWorker(project:" + project + ")"));
        context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(), jobEngineConfig.getConfig(), epochId);

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager.resumeAllRunningJobs();

        int pollSecond = jobEngineConfig.getPollIntervalSecond();
        logger.info("Fetching jobs every {} seconds", pollSecond);
        val fetcher = new FetcherRunner(this, jobPool, fetcherPool);

        if (config.isCheckQuotaStorageEnabled()) {
            fetcherPool.scheduleWithFixedDelay(new QuotaStorageCheckRunner(this), RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        }

        fetcherPool.scheduleWithFixedDelay(new JobCheckRunner(this), RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        fetcherPool.scheduleWithFixedDelay(new LicenseCapacityCheckRunner(this), RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        fetcherPool.scheduleWithFixedDelay(fetcher, RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        hasStarted.set(true);
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down DefaultScheduler for project {} ....", project);
        releaseResources();
        ExecutorServiceUtil.shutdownGracefully(fetcherPool, 60);
        ExecutorServiceUtil.shutdownGracefully(jobPool, 60);
    }

    public void forceShutdown() {
        logger.info("Shutting down DefaultScheduler ....");
        releaseResources();
        ExecutorServiceUtil.forceShutdown(fetcherPool);
        ExecutorServiceUtil.forceShutdown(jobPool);
    }

    private void releaseResources() {
        initialized.set(false);
        hasStarted.set(false);
        memoryRemaining.release(currentPrjUsingMemory.get());
    }

    @Override
    public boolean hasStarted() {
        return hasStarted.get();
    }

    public static double currentAvailableMem() {
        return 1.0 * memoryRemaining.availablePermits();
    }

}
