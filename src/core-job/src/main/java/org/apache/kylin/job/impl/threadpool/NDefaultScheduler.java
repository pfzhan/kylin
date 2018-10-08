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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 */
public class NDefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    private String project;
    private JobLock jobLock;
    private NExecutableManager executableManager;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;
    private static ConcurrentHashMap<String, Thread> threadToInterrupt = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(NDefaultScheduler.class);
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    volatile boolean fetchFailed = false;
    private JobEngineConfig jobEngineConfig;

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
                Map<String, Executable> runningJobs = context.getRunningJobs();
                if (isJobPoolFull()) {
                    return;
                }

                int nRunning = 0, nReady = 0, nStopped = 0, nOthers = 0, nError = 0, nDiscarded = 0, nSucceed = 0;
                for (final String path : executableManager.getJobPathes(project)) {
                    if (isJobPoolFull()) {
                        return;
                    }
                    if (runningJobs.containsKey(NExecutableManager.extractId(path))) {
                        nRunning++;
                        continue;
                    }
                    final Output output = executableManager.getOutputByJobPath(path);
                    if ((output.getState() != ExecutableState.READY)) {
                        if (output.getState() == ExecutableState.DISCARDED) {
                            nDiscarded++;
                        } else if (output.getState() == ExecutableState.ERROR) {
                            nError++;
                        } else if (output.getState() == ExecutableState.SUCCEED) {
                            nSucceed++;
                        } else if (output.getState() == ExecutableState.STOPPED) {
                            nStopped++;
                        } else {
                            if (fetchFailed) {
                                executableManager.forceKillJob(NExecutableManager.extractId(path));
                                nError++;
                            } else {
                                nOthers++;
                            }
                        }
                        continue;
                    }
                    nReady++;
                    AbstractExecutable executable = null;
                    String jobDesc = null;
                    try {
                        executable = executableManager.getJobByPath(path);
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

                fetchFailed = false;
                logger.info(
                        "Job Fetcher: {} should running, {} actual running, {} stopped, {} ready, {} already succeed, {} error, {} discarded, {} others",
                        nRunning, runningJobs.size(), nStopped, nReady, nSucceed, nError, nDiscarded, nOthers);
            } catch (Exception e) {
                fetchFailed = true;
                logger.warn("Job Fetcher caught a exception ", e);
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
    }

    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            try (SetThreadName ignored = new SetThreadName("Scheduler %s Job %s",
                    System.identityHashCode(NDefaultScheduler.this), executable.getId())) {
                threadToInterrupt.put(executable.getId(), Thread.currentThread());
                executable.execute(context);
                // trigger the next step asap
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
                context.removeRunningJob(executable);
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
            try {
                logger.info("ZK Connection state change to " + newState + ", shutdown default scheduler.");
                shutdown();
            } catch (SchedulerException e) {
                throw new RuntimeException("failed to shutdown scheduler", e);
            }
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

    public static synchronized void destroyInstance() {

        for (Map.Entry<String, NDefaultScheduler> entry : INSTANCE_MAP.entrySet()) {

            try {
                entry.getValue().shutdown();
            } catch (SchedulerException ex) {
                logger.error("Error shutting down NDefaultScheduler for project " + entry.getKey(), ex);
            }
        }
        INSTANCE_MAP.clear();
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, JobLock lock) throws SchedulerException {
        jobLock = lock;

        String serverMode = jobEngineConfig.getConfig().getServerMode();
        if (!("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase()))) {
            logger.info("server mode: " + serverMode + ", no need to run job scheduler");
            return;
        }
        logger.info("Initializing Job Engine ....");

        if (!initialized) {
            initialized = true;
        } else {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        if (jobLock.lockJobEngine() == false) {
            throw new IllegalStateException("Cannot start job scheduler due to lack of job lock");
        }

        executableManager = NExecutableManager.getInstance(jobEngineConfig.getConfig(), project);
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("NDefaultSchedulerFetchPool"));
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>(), new NamedThreadFactory("NDefaultSchedulerJobPool"));
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

        executableManager.resumeAllRunningJobs();

        int pollSecond = jobEngineConfig.getPollIntervalSecond();
        logger.info("Fetching jobs every {} seconds", pollSecond);
        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, pollSecond / 10, pollSecond, TimeUnit.SECONDS);
        hasStarted = true;
    }

    @Override
    public void shutdown() throws SchedulerException {
        logger.info("Shutting down DefaultScheduler ....");
        jobLock.unlockJobEngine();
        ExecutorServiceUtil.shutdownGracefully(fetcherPool, 60);
        ExecutorServiceUtil.shutdownGracefully(jobPool, 60);
    }

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

}
