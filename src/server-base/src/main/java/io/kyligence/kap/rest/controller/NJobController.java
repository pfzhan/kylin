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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import io.kyligence.kap.rest.service.JobService;

import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/jobs")
public class NJobController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NJobController.class);

    private static final Message msg = MsgPicker.getMsg();

    private Comparator<AbstractExecutable> lastModifyComparator = new Comparator<AbstractExecutable>() {
        @Override
        public int compare(AbstractExecutable o1, AbstractExecutable o2) {
            return new Long(o1.getLastModified()).compareTo(o2.getLastModified());
        }
    };

    private Comparator<AbstractExecutable> lastModifyComparatorReverse = new Comparator<AbstractExecutable>() {
        @Override
        public int compare(AbstractExecutable o1, AbstractExecutable o2) {
            return 0 - new Long(o1.getLastModified()).compareTo(o2.getLastModified());
        }
    };

    private Comparator<AbstractExecutable> startTimeComparator = new Comparator<AbstractExecutable>() {
        @Override
        public int compare(AbstractExecutable o1, AbstractExecutable o2) {
            return new Long(o1.getStartTime()).compareTo(o2.getStartTime());
        }
    };

    private Comparator<AbstractExecutable> startTimeComparatorReverse = new Comparator<AbstractExecutable>() {
        @Override
        public int compare(AbstractExecutable o1, AbstractExecutable o2) {
            return 0 - new Long(o1.getStartTime()).compareTo(o2.getStartTime());
        }
    };

    private Comparator<AbstractExecutable> durationComparator = new Comparator<AbstractExecutable>() {
        @Override
        public int compare(AbstractExecutable o1, AbstractExecutable o2) {
            return new Long(o1.getDuration()).compareTo(o2.getDuration());
        }
    };

    private Comparator<AbstractExecutable> durationComparatorReverse = new Comparator<AbstractExecutable>() {
        @Override
        public int compare(AbstractExecutable o1, AbstractExecutable o2) {
            return 0 - new Long(o1.getDuration()).compareTo(o2.getDuration());
        }
    };

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getJobList(@RequestParam(value = "status", required = false) Integer[] status,
            @RequestParam(value = "job_type", required = false) String jobType,
            @RequestParam(value = "timeFilter", required = false) Integer timeFilter,
            @RequestParam(value = "subjects", required = false) String[] subjects,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sortby", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse)
            throws IOException, PersistentException {
        checkProjectName(project);
        List<JobStatusEnum> statusList = new ArrayList<JobStatusEnum>();
        if (null != status || status.length == 0) {
            for (int stat : status) {
                statusList.add(JobStatusEnum.getByCode(stat));
            }
        }
        ArrayList<AbstractExecutable> executables = jobService.listJobs(project, statusList,
                JobTimeFilterEnum.getByCode(timeFilter), subjects, jobType);

        if (sortBy.equals("last_modify")) {
            if (reverse) {
                Collections.sort(executables, lastModifyComparatorReverse);
            } else {
                Collections.sort(executables, lastModifyComparator);
            }
        }
        if (sortBy.equals("start_time")) {
            if (reverse) {
                Collections.sort(executables, startTimeComparatorReverse);
            } else {
                Collections.sort(executables, startTimeComparator);
            }
        }

        if (sortBy.equals("duration")) {
            if (reverse) {
                Collections.sort(executables, durationComparatorReverse);
            } else {
                Collections.sort(executables, durationComparator);
            }
        }
        List<JobInstance> jobInstances = jobService.parseToJobInstance(executables, offset, limit, project);
        HashMap<String, Object> response = new HashMap<>();
        response.put("size", executables.size());
        response.put("jobs", jobInstances);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

}
