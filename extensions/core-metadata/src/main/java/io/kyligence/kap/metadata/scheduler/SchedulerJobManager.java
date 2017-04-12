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

package io.kyligence.kap.metadata.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.kyligence.kap.metadata.scheduler.SchedulerJobInstance.SCHEDULER_RESOURCE_ROOT;

public class SchedulerJobManager {
    public static final Serializer<SchedulerJobInstance> SCHEDULER_JOB_INSTANCE_SERIALIZER = new JsonSerializer<>(SchedulerJobInstance.class);
    private static final Logger logger = LoggerFactory.getLogger(SchedulerJobManager.class);

    private static final ConcurrentMap<KylinConfig, SchedulerJobManager> CACHE = new ConcurrentHashMap<>();
    private KylinConfig kylinConfig;
    private CaseInsensitiveStringCache<SchedulerJobInstance> jobMap;

    private SchedulerJobManager(KylinConfig config) throws IOException {
        logger.info("Initializing BadQueryHistoryManager with config " + config);
        this.kylinConfig = config;
        this.jobMap = new CaseInsensitiveStringCache<SchedulerJobInstance>(config, "scheduler");

        loadAllSchedulerJobInstance();
    }

    public static SchedulerJobManager getInstance(KylinConfig config) {
        SchedulerJobManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (SchedulerJobManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new SchedulerJobManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init SchedulerJobManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.kylinConfig);
    }

    private void loadAllSchedulerJobInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(SCHEDULER_RESOURCE_ROOT, ".json");

        logger.info("Loading scheduler jobs from folder " + store.getReadableResourcePath(ResourceStore.CUBE_RESOURCE_ROOT));

        int succeed = 0;
        int fail = 0;
        for (String path : paths) {
            SchedulerJobInstance job = reloadSchedulerJobLocalAt(path);
            if (job == null) {
                fail++;
            } else {
                succeed++;
            }
        }

        logger.info("Loaded " + succeed + " job, fail on " + fail + " jobs");
    }

    private synchronized SchedulerJobInstance reloadSchedulerJobLocalAt(String path) {
        ResourceStore store = getStore();
        SchedulerJobInstance job;

        try {
            job = store.getResource(path, SchedulerJobInstance.class, SCHEDULER_JOB_INSTANCE_SERIALIZER);
            checkNotNull(job, "job (at %s) not found", path);

            String jobName = job.getName();
            checkState(StringUtils.isNotBlank(jobName), "job (at %s) name must not be blank", path);

            String project = job.getProject();
            checkState(StringUtils.isNotBlank(project), "job (at %s) project must not be blank", path);

            String cube = job.getRelatedCube();
            checkState(StringUtils.isNotBlank(cube), "job (at %s) related cube must not be blank", path);

            String startTime = Long.toString(job.getPartitionStartTime());
            checkState(StringUtils.isNotBlank(startTime), "job (at %s) partition start time must not be blank", path);

            String scheduledRunTime = Long.toString(job.getScheduledRunTime());
            checkState(StringUtils.isNotBlank(scheduledRunTime), "job (at %s) scheduled run time must not be blank", path);

            String repeatCount = Long.toString(job.getRepeatCount());
            checkState(StringUtils.isNotBlank(repeatCount), "job (at %s) scheduled repeat count must not be blank", path);

            String curRepeatCount = Long.toString(job.getCurRepeatCount());
            checkState(StringUtils.isNotBlank(curRepeatCount), "job (at %s) current repeat count must not be blank", path);

            String repeatInterval = Long.toString(job.getRepeatInterval());
            checkState(StringUtils.isNotBlank(repeatInterval), "job (at %s) scheduled repeat interval must not be blank", path);

            String partitionInterval = Long.toString(job.getPartitionInterval());
            checkState(StringUtils.isNotBlank(partitionInterval), "job (at %s) scheduled partition interval must not be blank", path);

            jobMap.putLocal(jobName.toUpperCase(), job);

            return job;

        } catch (Exception e) {
            logger.error("Error during load scheduler job instance, skipping : " + path, e);
            return null;
        }
    }

    public SchedulerJobInstance getSchedulerJob(String name) throws IOException {
        name = name.toUpperCase();
        return jobMap.get(name);
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs() {
        return new ArrayList<SchedulerJobInstance>(jobMap.values());
    }

    public List<SchedulerJobInstance> getSchedulerJobs(String project, String cubeName) throws IOException {
        List<SchedulerJobInstance> list = listAllSchedulerJobs();
        List<SchedulerJobInstance> result = new ArrayList<SchedulerJobInstance>();
        Iterator<SchedulerJobInstance> it = list.iterator();

        while (it.hasNext()) {
            SchedulerJobInstance ci = it.next();
            if ((ci.getProject().equalsIgnoreCase(project) || project == null) && (ci.getRelatedCube().equalsIgnoreCase(cubeName) || cubeName == null)) {
                result.add(ci);
            }
        }
        return result;
    }

    public SchedulerJobInstance addSchedulerJob(SchedulerJobInstance job) throws IOException {
        if (job == null || StringUtils.isEmpty(job.getName())) {
            throw new IllegalArgumentException();
        }

        if (jobMap.containsKey(job.getName()))
            throw new IllegalArgumentException("Schedule job '" + job.getName() + "' already exists");

        String path = SchedulerJobInstance.concatResourcePath(job.getName());
        getStore().putResource(path, job, SCHEDULER_JOB_INSTANCE_SERIALIZER);
        jobMap.put(job.getName(), job);
        return job;
    }

    public void removeSchedulerJob(SchedulerJobInstance job) throws IOException {
        String path = job.getResourcePath();
        getStore().deleteResource(path);
        jobMap.remove(job.getName());
    }

    public SchedulerJobInstance updateSchedulerJobInstance(SchedulerJobInstance job) throws IOException {
        if (job.getName() == null || job.getRelatedCube() == null) {
            throw new IllegalArgumentException("Scheduler job illegal.");
        }

        String name = job.getName();
        if (!jobMap.containsKey(name)) {
            throw new IllegalArgumentException("Scheduler job '" + name + "' does not exist.");
        }

        // Save Source
        String path = job.getResourcePath();
        getStore().putResource(path, job, SCHEDULER_JOB_INSTANCE_SERIALIZER);

        jobMap.put(job.getName(), job);

        return job;
    }
}