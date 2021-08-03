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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
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

import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.response.AclTCRResponse;
import io.kyligence.kap.rest.service.AclTCRService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/acl", produces = { HTTP_VND_APACHE_KYLIN_JSON })
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

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @ApiOperation(value = "getProjectSidTCR", tags = { "MID" }, //
            notes = "Update URL: {project}; Update Param: project, authorized_only")
    @GetMapping(value = "/sid/{sid_type:.+}/{sid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<AclTCRResponse>> getProjectSidTCR(@PathVariable("sid_type") String sidType,
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestParam(value = "authorized_only", required = false, defaultValue = "false") boolean authorizedOnly)
            throws IOException {
        checkProjectName(project);
        List<AclTCRResponse> result;
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            result = getProjectSidTCR(project, sid, true, authorizedOnly);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            result = getProjectSidTCR(project, sid, false, authorizedOnly);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_SID_TYPE());
        }
        // remove new field `row_filter`
        result.stream().forEach(resp -> {
            Optional.ofNullable(resp.getTables()).orElse(Lists.newArrayList()).stream().forEach(table -> {
                table.setRowFilter(null);
            });
        });
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "getProjectSidTCR", tags = { "MID" }, //
            notes = "Update URL: {project}; Update Param: project, authorized_only")
    @GetMapping(value = "/{sid_type:.+}/{sid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<AclTCRResponse>> getProjectSidTCRV2(@PathVariable("sid_type") String sidType,
             @PathVariable("sid") String sid, //
             @RequestParam("project") String project, //
             @RequestParam(value = "authorized_only", required = false, defaultValue = "false") boolean authorizedOnly)
            throws IOException {
        checkProjectName(project);
        List<AclTCRResponse> result;
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            result = getProjectSidTCR(project, sid, true, authorizedOnly);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            result = getProjectSidTCR(project, sid, false, authorizedOnly);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_SID_TYPE());
        }
        // remove depreciated fields `rows` and `like_rows`
        result.stream().forEach(resp -> {
           Optional.ofNullable(resp.getTables()).orElse(Lists.newArrayList()).stream().forEach(table -> {
               table.setRows(null);
               table.setLikeRows(null);
           });
        });
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "updateProject", tags = { "MID" }, notes = "Update URL: {project}")
    @PutMapping(value = "/sid/{sid_type:.+}/{sid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> updateProject(@PathVariable("sid_type") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestBody List<AclTCRRequest> requests) throws IOException {
        checkProjectName(project);
        AclPermissionUtil.checkAclUpdatable(project, aclTCRService.getCurrentUserGroups());
        // Depreciated api can't use new field `row_filter`
        requests.stream().forEach(request -> Optional.ofNullable(request.getTables()).orElse(Lists.newArrayList())
                .stream().forEach(table -> {
            table.setRowFilter(null);
        }));
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            updateSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            updateSidAclTCR(project, sid, false, requests);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_SID_TYPE());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/updatable", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> getAllowAclUpdatable(@RequestParam("project") String project) throws IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                AclPermissionUtil.isAclUpdatable(project, aclTCRService.getCurrentUserGroups()), "");
    }

    private List<AclTCRResponse> getProjectSidTCR(String project, String sid, boolean principal, boolean authorizedOnly)
            throws IOException {
        accessService.checkSid(sid, principal);
        return aclTCRService.getAclTCRResponse(project, sid, principal, authorizedOnly);
    }

    private void updateSidAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        accessService.checkSid(sid, principal);
        boolean hasProjectPermission = accessService.hasProjectPermission(project, sid, principal);

        if (!hasProjectPermission) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(ACCESS_DENIED,
                    String.format(Locale.ROOT, msg.getGRANT_TABLE_WITH_SID_HAS_NOT_PROJECT_PERMISSION(), sid, project));
        }
        aclTCRService.updateAclTCR(project, sid, principal, requests);
    }
}
