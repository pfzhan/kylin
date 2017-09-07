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
import java.util.List;
import java.util.UUID;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.ExternalFilterRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.ExtFilterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

/**
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/extFilter")
public class ExternalFilterControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ExternalFilterControllerV2.class);

    @Autowired
    @Qualifier("extFilterService")
    private ExtFilterService extFilterService;

    @RequestMapping(value = "/saveExtFilter", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void saveExternalFilterV2(@RequestBody ExternalFilterRequest request) throws IOException {

        String filterProject = request.getProject();
        ExternalFilterDesc desc = JsonUtil.readValue(request.getExtFilter(), ExternalFilterDesc.class);
        desc.setUuid(UUID.randomUUID().toString());
        extFilterService.saveExternalFilter(desc);
        extFilterService.syncExtFilterToProject(new String[] { desc.getName() }, filterProject);
    }

    @RequestMapping(value = "/updateExtFilter", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateExternalFilterV2(@RequestBody ExternalFilterRequest request) throws IOException {

        ExternalFilterDesc desc = JsonUtil.readValue(request.getExtFilter(), ExternalFilterDesc.class);
        extFilterService.updateExternalFilter(desc);
        extFilterService.syncExtFilterToProject(new String[] { desc.getName() }, request.getProject());
    }

    @RequestMapping(value = "/{filter}/{project}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void removeFilterV2(@PathVariable String filter, @PathVariable String project) throws IOException {

        extFilterService.removeExtFilterFromProject(filter, project);
        extFilterService.removeExternalFilter(filter);
    }

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getExternalFiltersV2(@RequestParam(value = "project", required = true) String project)
            throws IOException {

        List<ExternalFilterDesc> filterDescs = Lists.newArrayList();
        filterDescs.addAll(extFilterService.getProjectManager().listExternalFilterDescs(project).values());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, filterDescs, "");
    }

}
