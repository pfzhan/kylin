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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.response.AclTCRResponse;
import io.kyligence.kap.rest.service.AclTCRService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/acl", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class AclTCRController extends NBasicController {

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @ApiOperation(value = "getProjectSidTCR (update)", notes = "Update URL: {project}; Update Param: project, authorized_only")
    @GetMapping(value = "/sid/{sid_type:.+}/{sid:.+}")
    @ResponseBody
    public EnvelopeResponse<List<AclTCRResponse>> getProjectSidTCR(@PathVariable("sid_type") String sidType,
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestParam(value = "authorized_only", required = false, defaultValue = "false") boolean authorizedOnly)
            throws IOException {
        checkProjectName(project);
        List<AclTCRResponse> result;
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            result = getProjectSidTCR(project, sid, true, authorizedOnly);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            result = getProjectSidTCR(project, sid, false, authorizedOnly);
        } else {
            result = Lists.newArrayList();
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "updateProject (update)", notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/sid/{sid_type:.+}/{sid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> updateProject(@PathVariable("sid_type") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestBody List<AclTCRRequest> requests) throws IOException {
        checkProjectName(project);
        AclPermissionUtil.checkAclUpdatable(project);
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            updateSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            updateSidAclTCR(project, sid, false, requests);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/updatable")
    @ResponseBody
    public EnvelopeResponse<Boolean> getAllowAclUpdatable(@RequestParam("project") String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, AclPermissionUtil.isAclUpdatable(project),
                "");
    }

    private List<AclTCRResponse> getProjectSidTCR(String project, String sid, boolean principal, boolean authorizedOnly)
            throws IOException {
        checkSid(sid, principal);
        return aclTCRService.getAclTCRResponse(project, sid, principal, authorizedOnly);
    }

    private void updateSidAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        checkSid(sid, principal);
        aclTCRService.updateAclTCR(project, sid, principal, requests);
    }

    @VisibleForTesting
    void checkSid(String sid, boolean principal) throws IOException {
        if (StringUtils.isEmpty(sid)) {
            throw new BadRequestException(MsgPicker.getMsg().getEMPTY_SID());
        }

        if (principal && !userService.userExists(sid)) {
            throw new BadRequestException("Operation failed, user:" + sid + " not exists, please add it first.");
        }
        if (!principal && !userGroupService.exists(sid)) {
            throw new BadRequestException("Operation failed, group:" + sid + " not exists, please add it first.");
        }
    }
}
