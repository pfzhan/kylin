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

import com.google.common.collect.Maps;
import io.kyligence.kap.cluster.ClusterManagerFactory;
import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.util.JobKiller;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingJobStatusWatcher {
    private static final Logger logger = LoggerFactory.getLogger(StreamingJobStatusWatcher.class);

    /**
     * hold the jobs whose status is stopping
     */
    private static Map<String, AtomicInteger> stoppingJobMap = Maps.newHashMap();
    /**
     * hold the jobs whose status is error or running
     */
    private static Map<String, AtomicInteger> jobMap = Maps.newHashMap();
    /**
     * keep the jobs whose process is killed
     */
    private static Map<String, Long> jobKeepMap = Maps.newHashMap();

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private boolean init = false;
    private static List<JobStatusEnum> STATUS_LIST = Arrays.asList(JobStatusEnum.ERROR, JobStatusEnum.RUNNING,
            JobStatusEnum.STOPPING);

    private static int WATCH_INTERVAL = 5;
    private static int JOB_KEEP_TIMEOUT = 30;

    public synchronized void schedule() {
        if (!init) {
            val config = KylinConfig.getInstanceFromEnv();
            if (StreamingUtils.isJobOnCluster() && "true".equals(config.getStreamingJobStatusWatchEnabled())) {
                scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    val runningJobs = getRunningJobs();
                    execute(runningJobs);
                }, WATCH_INTERVAL, WATCH_INTERVAL, TimeUnit.MINUTES);
            }
            init = true;
        }
    }

    private List<String> getRunningJobs() {
        val config = KylinConfig.getInstanceFromEnv();
        final IClusterManager cm = ClusterManagerFactory.create(config);
        val runningJobsOnYarn = cm.getRunningJobs(Collections.EMPTY_SET);
        return runningJobsOnYarn;
    }

    public synchronized void execute(List<String> runningJobsOnYarn) {
        val config = KylinConfig.getInstanceFromEnv();
        NProjectManager prjManager = NProjectManager.getInstance(config);
        val prjList = prjManager.listAllProjects();
        prjList.stream().forEach(projectInstance -> {
            val project = projectInstance.getName();
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            List<StreamingJobMeta> jobMetaList = mgr.listAllStreamingJobMeta();
            for (StreamingJobMeta meta : jobMetaList) {
                val jobId = StreamingUtils.getJobId(meta.getModelId(), meta.getJobType().name());
                if (jobKeepMap.containsKey(jobId)) {
                    val keepTime = System.currentTimeMillis() - jobKeepMap.get(jobId);
                    if (keepTime > JOB_KEEP_TIMEOUT * 60 * 1000) {
                        jobMap.remove(jobId);
                        jobKeepMap.remove(jobId);
                        continue;
                    }
                } else if (STATUS_LIST.contains(meta.getCurrentStatus())) {
                    if (runningJobsOnYarn.contains(jobId)) {
                        if (!jobMap.containsKey(jobId)) {
                            jobMap.put(jobId, new AtomicInteger(0));
                        } else if (jobMap.get(jobId).get() != 0) {
                            jobMap.get(jobId).set(0);
                        }
                    } else {
                        processMissingJobsFromYarn(meta, jobId);
                    }
                }
            }
        });
    }

    private void processMissingJobsFromYarn(StreamingJobMeta meta, String jobId) {
        String project = meta.getProject();
        if (jobMap.containsKey(jobId)) {
            killProcess(jobId, project, meta);
        } else {
            if (meta.getCurrentStatus() == JobStatusEnum.STOPPING) {
                if (!stoppingJobMap.containsKey(jobId)) {
                    stoppingJobMap.put(jobId, new AtomicInteger(0));
                } else {
                    stoppingJobMap.get(jobId).getAndIncrement();
                    if (stoppingJobMap.get(jobId).get() >= 3) {
                        jobMap.put(jobId, new AtomicInteger(0));
                        stoppingJobMap.remove(jobId);
                    }
                }
            } else if (meta.getCurrentStatus() == JobStatusEnum.RUNNING) {
                jobMap.put(jobId, new AtomicInteger(0));
            } else {
                String lastUpdateTime = meta.getLastUpdateTime();
                SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss",
                        Locale.getDefault(Locale.Category.FORMAT));
                try {
                    if (lastUpdateTime != null) {
                        val lastDateTime = simpleFormat.parse(lastUpdateTime);
                        val diff = (System.currentTimeMillis() - lastDateTime.getTime()) / (60 * 1000);
                        if (diff <= JOB_KEEP_TIMEOUT) {
                            jobMap.put(jobId, new AtomicInteger(0));
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private void killProcess(String jobId, String project, StreamingJobMeta meta) {
        val cnt = jobMap.get(jobId);
        if (cnt.get() < 3) {
            cnt.getAndIncrement();
        } else {
            logger.info("begin to find & kill jobId:" + jobId);
            val statusCode = JobKiller.killProcess(meta);
            logger.info(jobId + " statusCode=" + statusCode);
            if (meta.getCurrentStatus() != JobStatusEnum.ERROR) {
                MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.ERROR);
            }
            jobKeepMap.put(jobId, System.currentTimeMillis());
        }
    }
}
