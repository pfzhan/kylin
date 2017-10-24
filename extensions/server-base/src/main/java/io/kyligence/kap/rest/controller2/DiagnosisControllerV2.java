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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.DiagnosisService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.ExportQueryRequest;

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
     * Get slow query history
     */

    @RequestMapping(value = "/slow_query", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSlowQueries(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        Map<String, Object> data = dgService.getQueries(pageOffset, pageSize, BadQueryEntry.ADJ_SLOW,
                getBadQueryEntries(project));

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Get push-down query history
     */

    @RequestMapping(value = "/push_down", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getPushdownQueries(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        Map<String, Object> data = dgService.getQueries(pageOffset, pageSize, BadQueryEntry.ADJ_PUSHDOWN,
                getBadQueryEntries(project));

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

    @RequestMapping(value = "/export/push_down", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" }, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void exportPushdownQueries(ExportQueryRequest request, HttpServletResponse response) throws IOException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String nowStr = sdf.format(new Date());
        response.setContentType("text/plain;charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"query_pushdown_" + nowStr + ".sqls" + "\"");
        BufferedWriter bufferedWriter = null;

        try {
            Writer writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);

            bufferedWriter = new BufferedWriter(writer);

            List<BadQueryEntry> result = dgService.getQueriesByType(getBadQueryEntries(request.getProject()),
                    BadQueryEntry.ADJ_PUSHDOWN);

            if (request.getAll()) {
                for (BadQueryEntry entry : result) {
                    String toWrite = entry.getSql();
                    toWrite += ";";
                    bufferedWriter.write(toWrite);
                }
            } else {
                for (BadQueryEntry entry : result) {
                    if (request.getSelectedQueries().contains(entry.getUuid()) == false)
                        continue;
                    String toWrite = entry.getSql();
                    toWrite += "; \r\n";
                    bufferedWriter.write(toWrite);
                }
            }
            bufferedWriter.flush();

        } catch (IOException e) {
            throw new InternalErrorException(e);
        } finally {
            IOUtils.closeQuietly(bufferedWriter);
        }
    }

    private List<BadQueryEntry> getBadQueryEntries(String project) throws IOException {
        List<BadQueryEntry> allBadEntries = Lists.newArrayList();
        if (project != null) {
            BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(project);
            allBadEntries.addAll(badQueryHistory.getEntries());
        } else {
            for (ProjectInstance projectInstance : projectService.getReadableProjects()) {
                BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(projectInstance.getName());
                allBadEntries.addAll(badQueryHistory.getEntries());
            }
        }
        return allBadEntries;
    }

}
