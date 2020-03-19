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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.exception.JobStoppedVoluntarilyException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metric.SystemInfoCollector;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import lombok.Getter;
import lombok.val;

/**
 */
public class NDefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultScheduler.class);

    @Getter
    private String project;
    private JobLock jobLock;
    private FetcherRunner fetcher;
    private CheckerRunner checker;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private ExecutableContext context;
    private static ConcurrentHashMap<String, Thread> threadToInterrupt = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private volatile boolean isJobFull = false;
    private volatile boolean reachQuotaLimit = false;
    private JobEngineConfig jobEngineConfig;
    private ProjectStorageInfoCollector collector;
    private static volatile Semaphore memoryRemaining = new Semaphore(Integer.MAX_VALUE);

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

    public static void stopThread(String jobId) {
        Thread thread = threadToInterrupt.get(jobId);
        if (thread != null) {
            thread.interrupt();
            threadToInterrupt.remove(jobId);
        }
    }

    private boolean checkSuicide(String jobId) {
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (executableManager.getJob(jobId).getStatus().isFinalState()) {
            return false;
        }
        return executableManager.getJob(jobId).checkSuicide();
    }

    private boolean checkTimeoutIfNeeded(String jobId, Long startTime) {
        Integer timeOutMinute = KylinConfig.getInstanceFromEnv().getSchedulerJobTimeOutMinute();
        if (timeOutMinute == 0) {
            return false;
        }
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (executableManager.getJob(jobId).getStatus().isFinalState()) {
            return false;
        }
        long duration = System.currentTimeMillis() - startTime;
        long durationMins = Math.toIntExact(duration / (60 * 1000));
        return durationMins >= timeOutMinute;
    }

    private boolean discardSuicidalJob(String jobId) {
        try {
            if (checkSuicide(jobId)) {
                return UnitOfWork.doInTransactionWithRetry(() -> {
                    if (checkSuicide(jobId)) {
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).cancelJob(jobId);
                        return true;
                    }
                    return false;
                }, project);
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project " + project + " job " + jobId
                    + " should be suicidal but discard failed", e);
        }
        return false;
    }

    private boolean discardTimeoutJob(String jobId, Long startTime) {
        try {
            if (checkTimeoutIfNeeded(jobId, startTime)) {
                return UnitOfWork.doInTransactionWithRetry(() -> {
                    if (checkTimeoutIfNeeded(jobId, startTime)) {
                        logger.error("project {} job {} running timeout.", project, jobId);
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).errorJob(jobId);
                        return true;
                    }
                    return false;
                }, project);
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project " + project + " job " + jobId
                    + " should be timeout but discard failed", e);
        }
        return false;
    }

    private class CheckerRunner implements Runnable {

        @Override
        public void run() {
            logger.info("start check project {}", project);
            isJobFull = isJobPoolFull();
            reachQuotaLimit = reachStorageQuota();

            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Map<String, Executable> runningJobs = context.getRunningJobs();
            Map<String, Long> runningJobInfos = context.getRunningJobInfos();
            for (final String id : executableManager.getJobs()) {
                if (runningJobs.containsKey(id)) {
                    discardTimeoutJob(id, runningJobInfos.get(id));
                }
            }
        }

        private boolean isJobPoolFull() {
            Map<String, Executable> runningJobs = context.getRunningJobs();
            if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
                logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
                return true;
            }

            return false;
        }

        private boolean reachStorageQuota() {
            val storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(), project);
            val totalSize = storageVolumeInfo.getTotalStorageSize();
            val storageQuotaSize = storageVolumeInfo.getStorageQuotaSize();
            if (totalSize < 0) {
                logger.error(
                        "Project '{}' : an exception occurs when getting storage volume info, no job will be scheduled!!! The error info : {}",
                        project, storageVolumeInfo.getThrowableMap().get(StorageInfoEnum.TOTAL_STORAGE));
                return true;
            }
            if (totalSize >= storageQuotaSize) {
                logger.info("Project '{}' reach storage quota, no job will be scheduled!!!", project);
                return true;
            }
            return false;
        }
    }

    private class FetcherRunner implements Runnable {

        @Override
        public synchronized void run() {
            try {
                val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                Map<String, Executable> runningJobs = context.getRunningJobs();

                int nRunning = 0;
                int nReady = 0;
                int nStopped = 0;
                int nOthers = 0;
                int nError = 0;
                int nDiscarded = 0;
                int nSucceed = 0;
                int nSuicidal = 0;
                for (final String id : executableManager.getJobs()) {
                    if (discardSuicidalJob(id)) {
                        nDiscarded++;
                        continue;
                    }

                    if (runningJobs.containsKey(id)) {

                        // this is very important to prevent from same job being scheduled at same time.
                        // e.g. when a job is restarted, the old job may still be running (even if we tried to interrupt it)
                        // until the old job is finished, the new job should not start
                        nRunning++;
                        continue;
                    }

                    final Output output = executableManager.getOutput(id);
                    switch (output.getState()) {
                    case READY:
                        nReady++;
                        if (!isJobFull && !reachQuotaLimit) {
                            logger.info("fetcher schedule {} ", id);

                            scheduleJob(id);
                        }
                        break;
                    case DISCARDED:
                        nDiscarded++;
                        break;
                    case ERROR:
                        nError++;
                        break;
                    case SUCCEED:
                        nSucceed++;
                        break;
                    case PAUSED:
                        nStopped++;
                        break;
                    case SUICIDAL:
                        nSuicidal++;
                        break;
                    default:
                        nOthers++;
                        break;
                    }
                }

                logger.info(
                        "Job Status in project {}: {} should running, {} actual running, {} stopped, {} ready, {} already succeed, {} error, {} discarded, {} suicidal,  {} others",
                        project, nRunning, runningJobs.size(), nStopped, nReady, nSucceed, nError, nDiscarded,
                        nSuicidal, nOthers);
            } catch (Exception e) {
                logger.warn("Job Fetcher caught a exception ", e);
            }
        }

        private void scheduleJob(String id) {
            AbstractExecutable executable = null;
            String jobDesc = null;

            boolean memoryLock = false;
            int useMemoryCapacity = 0;
            try {
                val config = KylinConfig.getInstanceFromEnv();
                val executableManager = NExecutableManager.getInstance(config, project);
                executable = executableManager.getJob(id);
                useMemoryCapacity = executable.computeStepDriverMemory();

                memoryLock = memoryRemaining.tryAcquire(useMemoryCapacity);
                if (memoryLock) {
                    jobDesc = executable.toString();
                    logger.info("{} prepare to schedule", jobDesc);
                    context.addRunningJob(executable);
                    jobPool.execute(new JobRunner(executable));
                    logger.info("{} scheduled", jobDesc);
                } else {
                    logger.info("memory is not enough, remaining: {} MB", memoryRemaining.availablePermits());
                }
            } catch (Exception ex) {
                if (executable != null) {
                    context.removeRunningJob(executable);
                    if (memoryLock) {
                        // may release twice when exception raise after jobPool execute executable
                        memoryRemaining.release(useMemoryCapacity);
                    }
                }
                logger.warn(jobDesc + " fail to schedule", ex);
            }
        }
    }

    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            //only the first 8 chars of the job uuid
            try (SetThreadName ignored = new SetThreadName("JobWorker(prj:%s,jobid:%s)", project,
                    executable.getId().substring(0, 8))) {
                threadToInterrupt.put(executable.getId(), Thread.currentThread());
                executable.execute(context);
                // trigger the next step asap
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
            } catch (JobStoppedVoluntarilyException | JobStoppedNonVoluntarilyException e) {
                logger.info("Job quits either voluntarily or non-voluntarily", e);
            } catch (ExecuteException e) {
                logger.error("ExecuteException occurred while job: " + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job: " + executable.getId(), e);
            } finally {
                threadToInterrupt.remove(executable.getId());
                context.removeRunningJob(executable);
                memoryRemaining.release(executable.computeStepDriverMemory());
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
            logger.info("ZK Connection state change to {}, shutdown default scheduler.", newState);
            shutdown();
        }
    }

    public static synchronized NDefaultScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, NDefaultScheduler::new);
    }

    public void fetchJobsImmediately() {
        fetcherPool.schedule(new FetcherRunner(), 1, TimeUnit.SECONDS);
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
            instance.forceShutdown();
            INSTANCE_MAP.remove(project);

        }
    }

    public static synchronized NDefaultScheduler getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, JobLock lock) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        jobLock = lock;

        String serverMode = jobEngineConfig.getServerMode();
        if (!("job".equalsIgnoreCase(serverMode) || "all".equalsIgnoreCase(serverMode))) {
            logger.info("server mode: {}, no need to run job scheduler", serverMode);
            return;
        }
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing Job Engine ....");

        if (!initialized) {
            initialized = true;
        } else {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        if (!jobLock.lockJobEngine()) {
            throw new IllegalStateException("Cannot start job scheduler due to lack of job lock");
        }
        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.STORAGE_QUOTA, StorageInfoEnum.TOTAL_STORAGE);
        collector = new ProjectStorageInfoCollector(storageInfoEnumList);

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
        context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(), jobEngineConfig.getConfig());

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager.resumeAllRunningJobs();

        int pollSecond = jobEngineConfig.getPollIntervalSecond();
        logger.info("Fetching jobs every {} seconds", pollSecond);
        fetcher = new FetcherRunner();
        checker = new CheckerRunner();
        fetcherPool.scheduleWithFixedDelay(fetcher, RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        fetcherPool.scheduleWithFixedDelay(checker, RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        hasStarted = true;
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
        initialized = false;
        hasStarted = false;
        jobLock.unlockJobEngine();
    }

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

    public static double currentAvailableMem() {
        return 1.0 * memoryRemaining.availablePermits();
    }

}
