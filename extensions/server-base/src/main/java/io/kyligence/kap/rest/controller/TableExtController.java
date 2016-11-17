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

import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.service.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.service.TableExtService;

@Controller
@RequestMapping(value = "/table_ext")
public class TableExtController extends BasicController {

    @Autowired
    private TableExtService tableExtService;

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "/{tableName}", method = { RequestMethod.GET })
    @ResponseBody
    public TableExtDesc getTableExtDesc(@PathVariable String tableName) throws IOException {
        TableExtDesc tableExtDesc = tableExtService.getMetaDataManager().getTableExt(tableName);
        return tableExtDesc;
    }

    @RequestMapping(value = "/{project}/{tableName}", method = { RequestMethod.GET })
    @ResponseBody
    public JobInstance sample(@PathVariable String project, @PathVariable String tableName) throws IOException, JobException {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String jobID = tableExtService.extractTableExt(project, submitter, tableName);
        return jobService.getJobInstance(jobID);
    }
}
