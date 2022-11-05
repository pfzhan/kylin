/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.KylinException.CODE_SUCCESS;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ScheduleService;
import org.apache.kylin.rest.service.SystemService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.tool.util.ToolUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/system", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NSystemController extends NBasicController {

    @Autowired
    @Qualifier("systemService")
    private SystemService systemService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private ScheduleService scheduleService;

    @VisibleForTesting
    public void setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
    }

    @VisibleForTesting
    public AclEvaluate getAclEvaluate() {
        return this.aclEvaluate;
    }

    @PutMapping(value = "/roll_event_log")
    @ResponseBody
    public EnvelopeResponse<String> rollEventLog() {
        if (ToolUtil.waitForSparderRollUp()) {
            return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
        }
        return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", "Rollup sparder eventLog failed.");
    }

    @ApiOperation(value = "host", tags = { "DW" })
    @GetMapping(value = "/host")
    @ResponseBody
    public EnvelopeResponse<String> getHostname() {
        return new EnvelopeResponse<>(CODE_SUCCESS, AddressUtil.getLocalInstance(), "");
    }

    @ApiOperation(value = "reload metadata", tags = { "MID" })
    @PostMapping(value = "/metadata/reload")
    @ResponseBody
    public EnvelopeResponse<String> reloadMetadata() throws IOException {
        systemService.reloadMetadata();
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    //add UnitOfWork simulator
    @PostMapping(value = "/transaction/simulation")
    @ResponseBody
    public EnvelopeResponse<String> simulateUnitOfWork(String project, int seconds) {
        aclEvaluate.checkProjectAdminPermission(project);
        if (KylinConfig.getInstanceFromEnv().isUnitOfWorkSimulationEnabled()) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                long index = 0;
                while (index < seconds) {
                    index++;
                    Thread.sleep(1000L);
                }
                return index;
            }, project);
        }
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "cleanup garbage", tags = { "MID" })
    @PostMapping(value = "/do_cleanup_garbage")
    @ResponseBody
    public EnvelopeResponse<String> doCleanupGarbage(final HttpServletRequest request) throws Exception {
        scheduleService.routineTask();
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }
}
