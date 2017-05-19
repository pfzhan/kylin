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

package io.kyligence.kap.rest.controllerV2;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.TableServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.ExtTableRequest;
import io.kyligence.kap.rest.request.HiveTableExtRequest;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.source.hive.tablestats.HiveTableExtSampleJob;

@Controller
@RequestMapping(value = "/table_ext")
public class TableExtControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(TableExtControllerV2.class);

    @Autowired
    private TableExtService tableExtService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("tableServiceV2")
    private TableServiceV2 tableServiceV2;

    @RequestMapping(value = "/{database}.{tableName}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableExtDesc(@RequestHeader("Accept-Language") String lang, @PathVariable String database, @PathVariable String tableName) throws IOException {
        KapMsgPicker.setMsg(lang);

        TableExtDesc tableExtDesc = tableExtService.getTableExt(database + "." + tableName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableExtDesc, "");
    }

    @RequestMapping(value = "/{project}/{tableName}/sample_job", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse sample(@RequestHeader("Accept-Language") String lang, @PathVariable String project, @PathVariable String tableName, @RequestBody HiveTableExtRequest request) throws IOException {
        KapMsgPicker.setMsg(lang);

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String jobID = tableExtService.extractTableExt(project, submitter, request.getFrequency(), tableName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobID, "");
    }

    @RequestMapping(value = "/{tableName}/job", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listJob(@RequestHeader("Accept-Language") String lang, @PathVariable String tableName) throws IOException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        String jobID = tableExtService.getJobByTableName(tableName);
        if (jobID == null || jobID.isEmpty())
            throw new BadRequestException(msg.getJOB_INSTANCE_NOT_FOUND());
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

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadHiveTable(@RequestHeader("Accept-Language") String lang, @RequestBody ExtTableRequest request) throws IOException, JobException {
        KapMsgPicker.setMsg(lang);

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        boolean isCalculate = request.isNeedProfile();
        Map<String, String[]> loadResult = tableServiceV2.loadHiveTables(request.getTables(), request.getProject(), isCalculate);
        if (isCalculate) {

            String[] loadedTables = loadResult.get("result.loaded");
            for (String tableName : loadedTables)
                tableExtService.extractTableExt(request.getProject(), submitter, request.getFrequency(), tableName);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, loadResult, "");
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.DELETE }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unLoadHiveTables(@RequestHeader("Accept-Language") String lang, @PathVariable String tables, @PathVariable String project) throws Exception {
        String jobID;
        for (String tableName : tables.split(",")) {
            if ((jobID = new HiveTableExtSampleJob().findRunningJob(tableExtService.getConfig(), tableName)) != null) {
                jobService.cancelJob(jobService.getJobInstance(jobID));
            }
            tableExtService.removeTableExt(tableName);
        }

        String[] tableNames = StringUtil.splitAndTrim(tables, ",");
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableServiceV2.unloadHiveTables(tableNames, project), "");
    }
}
