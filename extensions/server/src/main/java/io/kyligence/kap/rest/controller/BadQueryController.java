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

package io.kyligence.kap.rest.controller;

import java.io.*;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.DiagnosisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Controller
@RequestMapping(value = "bquery")
public class BadQueryController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(BadQueryController.class);

    @Autowired
    private DiagnosisService dgService;

    /**
     * Get bad query history
     */
    @RequestMapping(value = "/{project}/sql", method = {RequestMethod.GET})
    @ResponseBody
    public List<BadQueryEntry> getBadQuerySql(@PathVariable String project) {

        List<BadQueryEntry> badEntry = Lists.newArrayList();
        try {
            BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(project);
            badEntry.addAll(badQueryHistory.getEntries());
        } catch (IOException e) {
            throw new InternalErrorException(e + " Caused by: " + e.getMessage(), e);
        }

        return badEntry;
    }

    /**
     * Get diagnosis information
     */
    @RequestMapping(value = "/{project}/download", method = {RequestMethod.GET})
    @ResponseBody
    public void getDiagnosisInfoFile(@PathVariable String project, final HttpServletRequest request, final HttpServletResponse response) {
        String filePath;
        try {
            filePath = dgService.dumpDiagnosisInfo(project);
        } catch (IOException e) {
            throw new InternalErrorException(e + " Caused by: " + e.getMessage(), e);
        }

        File file = new File(filePath);
        try (InputStream fileInputStream = new FileInputStream(file); OutputStream output = response.getOutputStream();) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.setContentLength((int) (file.length()));
            response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
            IOUtils.copyLarge(fileInputStream, output);
            output.flush();
        } catch (IOException e) {
            throw new InternalErrorException(e + " Caused by: " + e.getMessage(), e);
        }

    }
}
