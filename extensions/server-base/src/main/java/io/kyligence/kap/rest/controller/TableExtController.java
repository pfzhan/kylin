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
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.ExtTableRequest;
import io.kyligence.kap.rest.request.HiveTableExtRequest;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.util.ACLOperationUtil;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

@Controller
@RequestMapping(value = "/table_ext")
public class TableExtController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(TableExtController.class);

    @Autowired
    private TableExtService tableExtService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    // FIXME prj-table
    @RequestMapping(value = "/{project}/{database}.{tableName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableExtDesc(@PathVariable String database, @PathVariable String tableName, @PathVariable String project)
            throws IOException {

        TableExtDesc tableExtDesc = tableExtService.getTableExt(database + "." + tableName, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableExtDesc, "");
    }

    @RequestMapping(value = "/{project}/{tableName}/sample_job", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse sample(@PathVariable String project, @PathVariable String tableName,
            @RequestBody HiveTableExtRequest request) throws IOException {

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String jobID = tableExtService.extractTableExt(project, submitter, request.getFrequency(), tableName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobID, "");
    }

    // FIXME prj-table
    @RequestMapping(value = "/{project}/{tableName}/job", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listJob(@PathVariable String tableName, @PathVariable String project) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String jobID = tableExtService.getJobByTableName(tableName, project);
        if (jobID == null || jobID.isEmpty())
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
        try {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobService.getJobInstance(jobID), "");
        } catch (RuntimeException e) {
            /*
            By design, HiveTableExtSampleJob is moved from kap-engine-mr to kap-source-hive in kap2.3,
            therefore, kap2.3 or higher version can not parse kap2.2 stats job info.
             */
            logger.warn("Could not parse old version table stats job. job_id:{}, table_name:{}" + jobID + tableName);
        }
        throw new BadRequestException(msg.getJOB_INSTANCE_NOT_FOUND());
    }

    @RequestMapping(value = "/{project}/load", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadHiveTable(@RequestBody ExtTableRequest request) throws Exception {

        String project = request.getProject();
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        boolean isCalculate = request.isNeedProfile();

        List<String> tablesRunningJob = new ArrayList<>();
        List<String> tablesNotRunningJob = new ArrayList<>();
        for (String s : request.getTables()) {
            String jobID = new HiveTableExtSampleJob(project, s).findRunningJob();
            if (jobID != null) {
                tablesRunningJob.add(s);
            } else {
                tablesNotRunningJob.add(s);
            }
        }
        Map<String, String[]> loadResult = tableService.loadHiveTables(toArray(tablesNotRunningJob),
                request.getProject(), false);
        if (isCalculate) {
            String[] loadedTables = loadResult.get("result.loaded");
            for (String tableName : loadedTables)
                tableExtService.extractTableExt(request.getProject(), submitter, request.getFrequency(), tableName);
        }
        loadResult.put("result.running", toArray(tablesRunningJob));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, loadResult, "");
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unLoadHiveTables(@PathVariable String tables, @PathVariable String project)
            throws Exception {
        String jobID;
        for (String tableName : tables.split(",")) {
            ACLOperationUtil.delLowLevelACLByTbl(project, tableName);
            if ((jobID = new HiveTableExtSampleJob(project, tableName).findRunningJob()) != null) {
                jobService.cancelJob(jobService.getJobInstance(jobID));
            }
            tableExtService.removeTableExt(tableName, project);
        }

        String[] tableNames = StringUtil.splitAndTrim(tables, ",");
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableService.unloadHiveTables(tableNames, project), "");
    }

    private String[] toArray(List<String> list) {
        return list.toArray(new String[list.size()]);
    }
}
