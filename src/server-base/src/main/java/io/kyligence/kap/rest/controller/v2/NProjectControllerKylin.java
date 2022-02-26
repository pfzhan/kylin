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
package io.kyligence.kap.rest.controller.v2;

import java.util.List;

import io.kyligence.kap.rest.controller.NBasicController;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.service.ProjectService;
import io.swagger.annotations.ApiOperation;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_EARLY_JSON;

@Controller
@RequestMapping("/api/projects")
public class NProjectControllerKylin extends NBasicController {

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @ApiOperation(value = "getProjectsNoEnvelopeResponse", tags = { "SM" })
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_EARLY_JSON })
    @ResponseBody
    public List<ProjectInstance> getProjectsNoEnvelopeResponse(
            @RequestParam(value = "projectName", required = false) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer size,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch) {
        List<ProjectInstance> readableProjects = projectService.getReadableProjects(project, exactMatch);
        return (List<ProjectInstance>) getDataNoEnvelopeResponse(readableProjects, offset, size);
    }

}
