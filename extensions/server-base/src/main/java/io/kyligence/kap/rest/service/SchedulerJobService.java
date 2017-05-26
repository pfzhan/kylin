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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobManager;


@Component("schedulerJobService")
public class SchedulerJobService extends BasicService {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(SchedulerJobService.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    public SchedulerJobManager getSchedulerJobManager() {
        return SchedulerJobManager.getInstance(getConfig());
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs(final String projectName, final String cubeName, final Integer limit, final Integer offset) throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().getSchedulerJobs(projectName, cubeName);

        int climit = (null == limit) ? Integer.MAX_VALUE : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (jobs.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - coffset) < climit) {
            return jobs.subList(coffset, jobs.size());
        }

        return jobs.subList(coffset, coffset + climit);
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs(final String projectName, final String cubeName) throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().getSchedulerJobs(projectName, cubeName);
        return jobs;
    }

    public List<SchedulerJobInstance> listAllSchedulerJobs() throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().listAllSchedulerJobs();
        return jobs;
    }

    public SchedulerJobInstance getSchedulerJob(final String jobName) throws IOException {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(jobName);

        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance saveSchedulerJob(SchedulerJobInstance job) throws IOException {
        getSchedulerJobManager().addSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance saveSchedulerJob(String name, String project, String cube, long triggerTime, long startTime, long repeatCount, long curRepeatCount, long repeatInterval, long partitionInterval) throws IOException {

        SchedulerJobInstance job = new SchedulerJobInstance(name, project, cube, startTime, triggerTime, repeatCount, curRepeatCount, repeatInterval, partitionInterval);
        getSchedulerJobManager().addSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance updateSchedulerJob(String name, Map<String, Long> settings) throws Exception {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(name);

        for (String key : settings.keySet()) {

            switch (key) {
            case "partitionStartTime":
                job.setPartitionStartTime(settings.get(key));
                break;
            case "scheduledRunTime":
                job.setScheduledRunTime(settings.get(key));
                break;
            case "repeatCount":
                job.setRepeatCount(settings.get(key));
                break;
            case "curRepeatCount":
                job.setCurRepeatCount(settings.get(key));
                break;
            case "repeatInterval":
                job.setRepeatInterval(settings.get(key));
                break;
            case "setPartitionInterval":
                job.setPartitionInterval(settings.get(key));
                break;
            default:
                throw new Exception("Unrecognized key " + key + ".");
            }
        }

        getSchedulerJobManager().updateSchedulerJobInstance(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance deleteSchedulerJob(String name) throws IOException {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(name);
        getSchedulerJobManager().removeSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance deleteSchedulerJob(SchedulerJobInstance job) throws IOException {
        getSchedulerJobManager().removeSchedulerJob(job);
        return job;
    }
}
