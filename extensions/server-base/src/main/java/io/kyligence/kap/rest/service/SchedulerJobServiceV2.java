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

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

/**
 * Created by luwei on 17-4-29.
 */
@Component("schedulerJobServiceV2")
public class SchedulerJobServiceV2 extends SchedulerJobService {

    public List<SchedulerJobInstance> listAllSchedulerJobs(final String projectName, final String cubeName) throws IOException {
        List<SchedulerJobInstance> jobs = getSchedulerJobManager().getSchedulerJobs(projectName, cubeName);
        return jobs;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance saveSchedulerJob(String name, String project, String cube, long triggerTime, long startTime, long repeatCount, long curRepeatCount, long repeatInterval, long partitionInterval) throws IOException {

        SchedulerJobInstance job = new SchedulerJobInstance(name, project, cube, startTime, triggerTime, repeatCount, curRepeatCount, repeatInterval, partitionInterval);
        getSchedulerJobManager().addSchedulerJob(job);
        return job;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public SchedulerJobInstance deleteSchedulerJob(String name) throws IOException {
        SchedulerJobInstance job = getSchedulerJobManager().getSchedulerJob(name);
        getSchedulerJobManager().removeSchedulerJob(job);
        return job;
    }
}
