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
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.response.AclTCRResponse;
import io.kyligence.kap.rest.service.AclTCRService;

@Controller
@RequestMapping(value = "/acl/{project}", produces = { "application/vnd.apache.kylin-v2+json" })
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

    private static final Pattern sidPattern = Pattern.compile("^[a-zA-Z0-9_]*$");

    @GetMapping(value = "/sid/{sidType}/{sid}")
    @ResponseBody
    public EnvelopeResponse getProjectSidTCR(@PathVariable("project") String project, //
            @PathVariable("sidType") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestParam(value = "authorizedOnly", required = false, defaultValue = "false") boolean authorizedOnly)
            throws IOException {
        List<AclTCRResponse> result;
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            result = getProjectSidTCR(project, sid, true, authorizedOnly);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            result = getProjectSidTCR(project, sid, false, authorizedOnly);
        } else {
            result = Lists.newArrayList();
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @PostMapping(value = "/sid/{sidType}/{sid}")
    @ResponseBody
    public EnvelopeResponse updateProject(@PathVariable("project") String project, //
            @PathVariable("sidType") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestBody List<AclTCRRequest> requests) throws IOException {
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            updateSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            updateSidAclTCR(project, sid, false, requests);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
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

    private void checkSid(String sid, boolean principal) throws IOException {
        if (StringUtils.isEmpty(sid)) {
            throw new BadRequestException(MsgPicker.getMsg().getEMPTY_SID());
        }
        if (!sidPattern.matcher(sid).matches()) {
            throw new BadRequestException(MsgPicker.getMsg().getINVALID_SID());
        }

        if (principal && !userService.userExists(sid)) {
            throw new BadRequestException("Operation failed, user:" + sid + " not exists, please add it first.");
        }
        if (!principal && !userGroupService.exists(sid)) {
            throw new BadRequestException("Operation failed, group:" + sid + " not exists, please add it first.");
        }
    }
}
