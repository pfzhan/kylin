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
import org.apache.kylin.job.exception.JobSuicideException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import lombok.Getter;
import lombok.val;

/**
 */
public class NDefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    @Getter
    private String project;
    private JobLock jobLock;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;
    private static ConcurrentHashMap<String, Thread> threadToInterrupt = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(NDefaultScheduler.class);
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private JobEngineConfig jobEngineConfig;
    private ProjectStorageInfoCollector collector;

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
                    if (runningJobs.containsKey(id)) {
                        nRunning++;
                        continue;
                    }
                    final Output output = executableManager.getOutput(id);
                    switch (output.getState()) {
                    case READY:
                        nReady++;
                        if (!isJobPoolFull() && !reachStorageQuota()) {
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

        private boolean reachStorageQuota() {
            val storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(), project);
            val totalSize = storageVolumeInfo.getTotalStorageSize();
            val storageQuotaSize = storageVolumeInfo.getStorageQuotaSize();
            if (totalSize < 0) {
                logger.error(String.format(
                        "Project '%s' : an exception occurs when getting storage volume info, no job will be scheduled!!! The error info : %s",
                        project, storageVolumeInfo.getThrowableMap().get(StorageInfoEnum.TOTAL_STORAGE)));
                return true;
            }
            if (totalSize >= storageQuotaSize) {
                logger.info(String.format("Project '%s' reach storage quota, no job will be scheduled!!!", project));
                return true;
            }
            return false;
        }

        private boolean isJobPoolFull() {
            Map<String, Executable> runningJobs = context.getRunningJobs();
            if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
                logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
                return true;
            }

            return false;
        }

        private void scheduleJob(String id) {
            AbstractExecutable executable = null;
            String jobDesc = null;
            try {
                val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                executable = executableManager.getJob(id);
                jobDesc = executable.toString();
                logger.info("{} prepare to schedule", jobDesc);
                context.addRunningJob(executable);
                jobPool.execute(new JobRunner(executable));
                logger.info("{} scheduled", jobDesc);
            } catch (Exception ex) {
                if (executable != null)
                    context.removeRunningJob(executable);
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
            try (SetThreadName ignored = new SetThreadName("JobWorker(project:%s,jobidprefix:%s)", project,
                    executable.getId().substring(0, 8))) {
                threadToInterrupt.put(executable.getId(), Thread.currentThread());
                executable.execute(context);
                // trigger the next step asap
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
            } catch (JobSuicideException e) {
                logger.info("job " + executable.getId() + " suicides as its serving model/segment no longer exists", e);
            } catch (ExecuteException e) {
                logger.error("ExecuteException occurred while job: " + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job: " + executable.getId(), e);
            } finally {
                threadToInterrupt.remove(executable.getId());
                context.removeRunningJob(executable);
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
        NDefaultScheduler ret = INSTANCE_MAP.get(project);
        if (ret == null) {
            ret = new NDefaultScheduler(project);
            INSTANCE_MAP.put(project, ret);
        }
        return ret;
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
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<>(), new NamedThreadFactory("RunJobWorker(project:" + project + ")"));
        context = new DefaultContext(Maps.newConcurrentMap(), jobEngineConfig.getConfig());

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager.resumeAllRunningJobs();

        int pollSecond = jobEngineConfig.getPollIntervalSecond();
        logger.info("Fetching jobs every {} seconds", pollSecond);
        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        hasStarted = true;
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down DefaultScheduler for project {} ....", project);
        jobLock.unlockJobEngine();
        ExecutorServiceUtil.shutdownGracefully(fetcherPool, 60);
        ExecutorServiceUtil.shutdownGracefully(jobPool, 60);
    }

    public void forceShutdown() {
        logger.info("Shutting down DefaultScheduler ....");
        jobLock.unlockJobEngine();
        ExecutorServiceUtil.forceShutdown(fetcherPool);
        ExecutorServiceUtil.forceShutdown(jobPool);
    }

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

}
