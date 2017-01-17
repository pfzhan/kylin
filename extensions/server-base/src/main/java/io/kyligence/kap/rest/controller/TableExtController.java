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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.controller.TableController;
import org.apache.kylin.rest.request.HiveTableRequest;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

@Controller
@RequestMapping(value = "/table_ext")
public class TableExtController extends BasicController {

    @Autowired
    private TableExtService tableExtService;

    @Autowired
    private JobService jobService;

    @Autowired
    private TableController tableController;

    @RequestMapping(value = "/{database}.{tableName}", method = { RequestMethod.GET })
    @ResponseBody
    public TableExtDesc getTableExtDesc(@PathVariable String database, @PathVariable String tableName) throws IOException {
        TableExtDesc tableExtDesc = tableExtService.getTableExt(database + "." + tableName);
        return tableExtDesc;
    }

    @RequestMapping(value = "/{project}/{tableName}/sample_job", method = { RequestMethod.PUT })
    @ResponseBody
    public List<JobInstance> sample(@PathVariable String project, @PathVariable String tableName) throws IOException, JobException {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        List<String> jobIDs = tableExtService.extractTableExt(project, submitter, tableName);
        List<JobInstance> jobInstanceList = new ArrayList<>();
        for (String jobID : jobIDs) {
            jobInstanceList.add(jobService.getJobInstance(jobID));
        }
        return jobInstanceList;
    }

    @RequestMapping(value = "/{tableName}/job", method = { RequestMethod.GET })
    @ResponseBody
    public JobInstance listJob(@PathVariable String tableName) throws IOException, JobException {
        String jobID = tableExtService.getJobByTableName(tableName);
        if (jobID == null || jobID.isEmpty())
            return null;
        return jobService.getJobInstance(jobID);
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.POST })
    @ResponseBody
    public Map<String, String[]> loadHiveTable(@PathVariable String tables, @PathVariable String project, @RequestBody HiveTableRequest request) throws IOException, JobException {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        boolean isCalculate = request.isCalculate();
        request.setCalculate(false);
        Map<String, String[]> loadResult = tableController.loadHiveTables(tables, project, request);
        if (isCalculate) {
            String[] loadedTables = loadResult.get("result.loaded");
            List<String> jobIDs = tableExtService.extractTableExt(project, submitter, loadedTables);
        }
        return loadResult;
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.DELETE })
    @ResponseBody
    public Map<String, String[]> unLoadHiveTables(@PathVariable String tables, @PathVariable String project) throws Exception {
        String jobID;
        for (String tableName : tables.split(",")) {
            if ((jobID = HiveTableExtSampleJob.findRunningJob(tableName, tableExtService.getConfig())) != null) {
                jobService.cancelJob(jobService.getJobInstance(jobID));
            }
            tableExtService.removeTableExt(tableName);
        }
        Map<String, String[]> unloadResult = tableController.unLoadHiveTables(tables, project);
        return unloadResult;
    }
}
