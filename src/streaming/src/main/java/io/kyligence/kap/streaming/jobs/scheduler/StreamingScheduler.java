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

package io.kyligence.kap.streaming.jobs.scheduler;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.jobs.thread.StreamingJobRunner;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.util.JobKiller;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingScheduler {

    @Getter
    private String project;

    @Getter
    private AtomicBoolean initialized = new AtomicBoolean(false);

    @Getter
    private AtomicBoolean hasStarted = new AtomicBoolean(false);

    private ExecutorService jobPool;
    private Map<String, StreamingJobRunner> runnerMap = Maps.newHashMap();
    private Map<String, AbstractMap.SimpleEntry<AtomicInteger, AtomicInteger>> retryMap = Maps.newHashMap();

    private static final Map<String, StreamingScheduler> INSTANCE_MAP = Maps.newConcurrentMap();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private static final List<JobStatusEnum> STARTABLE_STATUS_LIST = Arrays.asList(JobStatusEnum.ERROR, JobStatusEnum.STOPPED,
            JobStatusEnum.NEW, JobStatusEnum.READY, JobStatusEnum.LAUNCHING_ERROR);

    private static StreamingJobStatusWatcher jobStatusUpdater = new StreamingJobStatusWatcher();

    public StreamingScheduler(String project) {
        Preconditions.checkNotNull(project);
        this.project = project;

        if (INSTANCE_MAP.containsKey(project))
            throw new IllegalStateException(
                    "StreamingScheduler for project " + project + " has been initiated. Use getInstance() instead.");
        init();
    }

    public static synchronized StreamingScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, StreamingScheduler::new);
    }

    public synchronized void init() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        if (!config.isJobNode() && !config.isDataLoadingNode()) {
            log.info("server mode: {}, no need to run job scheduler", config.getServerMode());
            return;
        }
        if (!UnitOfWork.isAlreadyInTransaction())
            log.info("Initializing Job Engine ....");

        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        if (config.streamingEnabled()) {
            int maxPoolSize = config.getMaxStreamingConcurrentJobLimit();
            ThreadFactory executorThreadFactory = new BasicThreadFactory.Builder()
                    .namingPattern("StreamingJobWorker(project:" + project + ")").uncaughtExceptionHandler((t, e) -> {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }).build();

            jobPool = new ThreadPoolExecutor(maxPoolSize, maxPoolSize * 2, Long.MAX_VALUE, TimeUnit.DAYS,
                    new SynchronousQueue<>(), executorThreadFactory);
            log.debug("New StreamingScheduler created by project '{}': {}", project,
                    System.identityHashCode(StreamingScheduler.this));
            scheduledExecutorService.scheduleWithFixedDelay(this::retryJob, 5, 1, TimeUnit.MINUTES);
            jobStatusUpdater.schedule();
        }
        resumeJobs(config);
        hasStarted.set(true);
    }

    public synchronized void submitJob(String project, String modelId, JobTypeEnum jobType) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.streamingEnabled()) {
            return;
        }
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());
        var jobMeta = StreamingJobManager.getInstance(config, project).getStreamingJobByUuid(jobId);
        checkJobStartStatus(jobMeta, jobId);
        JobKiller.killProcess(jobMeta);

        killYarnApplication(jobId, modelId);

        Predicate<NDataSegment> predicate = item -> (item.getStatus() == SegmentStatusEnum.NEW
                || item.getStorageBytesSize() == 0) && item.getAdditionalInfo() != null;
        if (JobTypeEnum.STREAMING_BUILD == jobType) {
            deleteBrokenSegment(project, modelId, item -> predicate.apply(item)
                    && !item.getAdditionalInfo().containsKey(StreamingConstants.FILE_LAYER));
        } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
            deleteBrokenSegment(project, modelId, item -> predicate.apply(item)
                    && item.getAdditionalInfo().containsKey(StreamingConstants.FILE_LAYER));
        }
        MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.STARTING);
        StreamingJobRunner jobRunner = new StreamingJobRunner(project, modelId, jobType);
        runnerMap.put(jobId, jobRunner);
        jobPool.execute(jobRunner);
        if (!StreamingUtils.isJobOnCluster(config)) {
            MetaInfoUpdater.updateJobState(project, jobId, Sets.newHashSet(JobStatusEnum.RUNNING, JobStatusEnum.ERROR),
                    JobStatusEnum.RUNNING);
        }
    }

    public synchronized void stopJob(String modelId, JobTypeEnum jobType) {
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        boolean existed = applicationExisted(jobId);
        if (existed) {
            StreamingJobManager jobMgr = StreamingJobManager.getInstance(config, project);
            JobStatusEnum status = jobMgr.getStreamingJobByUuid(jobId).getCurrentStatus();
            if (JobStatusEnum.ERROR == status || JobStatusEnum.STOPPED == status) {
                return;
            }
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.STOPPING);
            doStop(modelId, jobType);
        } else {
            doStop(modelId, jobType);
            if (StreamingUtils.isJobOnCluster(config)) {
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.STOPPED, JobStatusEnum.ERROR), JobStatusEnum.ERROR);
            } else {
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.STOPPED, JobStatusEnum.ERROR), JobStatusEnum.STOPPED);
            }
        }
    }

    private void doStop(String modelId, JobTypeEnum jobType) {
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());
        StreamingJobRunner runner = runnerMap.get(jobId);
        synchronized (runnerMap) {
            if (Objects.isNull(runner)) {
                runner = new StreamingJobRunner(project, modelId, jobType);
                runner.init();
                runnerMap.put(jobId, runner);
            }
        }
        runner.stop();
    }

    public static synchronized void shutdownByProject(String project) {
        val instance = INSTANCE_MAP.get(project);
        if (instance != null) {
            INSTANCE_MAP.remove(project);
            instance.forceShutdown();
        }
    }

    public void forceShutdown() {
        log.info("Shutting down DefaultScheduler ....");
        releaseResources();
        ExecutorServiceUtil.forceShutdown(scheduledExecutorService);
        ExecutorServiceUtil.forceShutdown(jobPool);
    }

    private void releaseResources() {
        initialized.set(false);
        hasStarted.set(false);
        INSTANCE_MAP.remove(project);
    }

    private void checkJobStartStatus(StreamingJobMeta jobMeta, String jobId) {
        if (!STARTABLE_STATUS_LIST.contains(jobMeta.getCurrentStatus())) {
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, jobId);
        }
    }

    public void retryJob() {
        val config = KylinConfig.getInstanceFromEnv();

        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        List<StreamingJobMeta> jobMetaList = mgr.listAllStreamingJobMeta();
        List<StreamingJobMeta> retryJobMetaList = jobMetaList
                .stream().filter(meta -> "true".equals(meta.getParams()
                        .getOrDefault(StreamingConstants.STREAMING_RETRY_ENABLE, config.getStreamingJobRetryEnabled())))
                .collect(Collectors.toList());
        retryJobMetaList.forEach(meta -> {
            JobStatusEnum status = meta.getCurrentStatus();
            String modelId = meta.getModelId();
            String jobId = StreamingUtils.getJobId(modelId, meta.getJobType().name());
            if (retryMap.containsKey(jobId) || status == JobStatusEnum.ERROR) {
                boolean canRestart = !applicationExisted(jobId);
                if (canRestart) {
                    if (!retryMap.containsKey(jobId)) {
                        if (status == JobStatusEnum.ERROR) {
                            retryMap.put(jobId, new AbstractMap.SimpleEntry<>(
                                    new AtomicInteger(config.getStreamingJobRetryInterval()), new AtomicInteger(1)));
                        }
                    } else {
                        int targetCnt = retryMap.get(jobId).getKey().get();
                        int currCnt = retryMap.get(jobId).getValue().get();
                        log.debug("targetCnt=" + targetCnt + ",currCnt=" + currCnt + " jobId=" + jobId);

                        if (targetCnt <= config.getStreamingJobMaxRetryInterval()) {
                            retryMap.get(jobId).getValue().incrementAndGet();
                            if (targetCnt == currCnt && status == JobStatusEnum.ERROR) {
                                log.info("begin to restart job:" + modelId + "_" + meta.getJobType());
                                restartJob(config, meta, jobId, targetCnt);
                            }
                        }

                    }
                } else {
                    if (status == JobStatusEnum.RUNNING && retryMap.containsKey(jobId)) {
                        log.debug("remove jobId=" + jobId);
                        retryMap.remove(jobId);
                    }
                }
            }
        });
    }

    private void restartJob(KylinConfig config, StreamingJobMeta meta, String jobId, int targetCnt) {
        try {
            submitJob(meta.getProject(), meta.getModelId(), meta.getJobType());
            if (targetCnt < config.getStreamingJobMaxRetryInterval()) {
                retryMap.get(jobId).getKey().addAndGet(config.getStreamingJobRetryInterval());
            }
            retryMap.get(jobId).getValue().set(0);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void deleteBrokenSegment(String project, String dataflowId, Predicate<NDataSegment> predicate) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dfMgr.getDataflow(dataflowId);
            Segments<NDataSegment> segments = df.getSegments();
            List<NDataSegment> toRemoveSegs = segments.stream().filter(item -> predicate.apply(item))
                    .collect(Collectors.toList());
            if (!toRemoveSegs.isEmpty()) {
                val dfUpdate = new NDataflowUpdate(dataflowId);
                dfUpdate.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));
                dfMgr.updateDataflow(dfUpdate);
            }
            return 0;
        }, project);
    }

    public boolean applicationExisted(String jobId) {
        return JobKiller.applicationExisted(jobId);
    }

    public void killYarnApplication(String jobId, String modelId) {
        boolean isExists = applicationExisted(jobId);
        if (isExists) {
            JobKiller.killApplication(jobId);
            isExists = applicationExisted(jobId);
            if (isExists) {
                String model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .getDataModelDesc(modelId).getAlias();
                throw new KylinException(ServerErrorCode.REPEATED_START_ERROR,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getJobStartFailure(), model));
            }
        }
    }

    private void killJob(String modelId, JobTypeEnum jobTypeEnum) {
        killJob(modelId, jobTypeEnum, JobStatusEnum.ERROR);
    }

    public void killJob(String modelId, JobTypeEnum jobTypeEnum, JobStatusEnum status) {
        killJob(StreamingUtils.getJobId(modelId, jobTypeEnum.name()), status);
    }

    private void killJob(String jobId, JobStatusEnum status) {
        val config = KylinConfig.getInstanceFromEnv();
        var jobMeta = StreamingJobManager.getInstance(config, project).getStreamingJobByUuid(jobId);
        JobKiller.killProcess(jobMeta);
        JobKiller.killApplication(jobId);

        MetaInfoUpdater.updateJobState(project, jobId, status);
    }

    private void resumeJobs(KylinConfig config) {
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        List<StreamingJobMeta> jobMetaList = mgr.listAllStreamingJobMeta();

        if (CollectionUtils.isEmpty(jobMetaList)) {
            return;
        }
        List<StreamingJobMeta> retryJobMetaList = jobMetaList.stream()
                .filter(meta -> JobStatusEnum.STARTING == meta.getCurrentStatus()
                        || JobStatusEnum.STOPPING == meta.getCurrentStatus()
                        || JobStatusEnum.RUNNING == meta.getCurrentStatus()
                        || JobStatusEnum.ERROR == meta.getCurrentStatus())
                .collect(Collectors.toList());
        retryJobMetaList.forEach(meta -> {
            val modelId = meta.getModelId();
            val jobType = meta.getJobType();
            if (meta.isSkipListener()) {
                skipJobListener(project, StreamingUtils.getJobId(modelId, jobType.name()), false);
            }
            if (JobStatusEnum.RUNNING == meta.getCurrentStatus() || JobStatusEnum.STARTING == meta.getCurrentStatus()) {
                killJob(meta.getModelId(), meta.getJobType(), JobStatusEnum.STOPPED);
                submitJob(project, modelId, jobType);
            } else {
                killJob(meta.getModelId(), meta.getJobType());
            }
        });
    }


    public void skipJobListener(String project, String uuid, boolean skip) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            val mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(uuid, copyForWrite -> {
                if (copyForWrite != null) {
                    copyForWrite.setSkipListener(skip);
                }
            });
            return null;
        }, project);
    }

}
