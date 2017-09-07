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

package io.kyligence.kap.rest.controller2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.DiagnosisService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

@Controller
@RequestMapping(value = "/diag")
public class DiagnosisControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosisControllerV2.class);

    @Autowired
    @Qualifier("diagnosisService")
    private DiagnosisService dgService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    /**
     * Get bad query history
     */

    @RequestMapping(value = "/sql", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getBadQuerySqlV2(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        List<BadQueryEntry> badEntry = Lists.newArrayList();
        if (project != null) {
            BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(project);
            badEntry.addAll(badQueryHistory.getEntries());
        } else {
            for (ProjectInstance projectInstance : projectService.getReadableProjects()) {
                BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(projectInstance.getName());
                badEntry.addAll(badQueryHistory.getEntries());
            }
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (badEntry.size() <= offset) {
            offset = badEntry.size();
            limit = 0;
        }

        if ((badEntry.size() - offset) < limit) {
            limit = badEntry.size() - offset;
        }

        data.put("badQueries", badEntry.subList(offset, offset + limit));
        data.put("size", badEntry.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Get diagnosis information for project
     */

    @RequestMapping(value = "/project/{project}/download", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void dumpProjectDiagnosisInfoV2(@PathVariable String project, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        String filePath;
        filePath = dgService.dumpProjectDiagnosisInfo(project);

        setDownloadResponse(filePath, response);
    }

    /**
     * Get diagnosis information for job
     */

    @RequestMapping(value = "/job/{jobId}/download", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void dumpJobDiagnosisInfoV2(@PathVariable String jobId, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        String filePath;
        filePath = dgService.dumpJobDiagnosisInfo(jobId);

        setDownloadResponse(filePath, response);
    }
}
