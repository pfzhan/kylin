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

package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/acl", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenAclTCRController extends NBasicController {

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @ApiOperation(value = "updateProjectAcl", tags = { "MID" }, notes = "Update URL: {project}; Update Param: project")
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
            mergeSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            mergeSidAclTCR(project, sid, false, requests);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void mergeSidAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        accessService.checkSid(sid, principal);
        boolean hasProjectPermission = accessService.hasProjectPermission(project, sid, principal);

        if (!hasProjectPermission) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(ACCESS_DENIED,
                    String.format(Locale.ROOT, msg.getGrantTableWithSidHasNotProjectPermission(), sid, project));
        }
        aclTCRService.mergeAclTCR(project, sid, principal, requests);
    }

    @ApiOperation(value = "updateProjectAcl With New Foramt", tags = {
            "MID" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/batch/{sid_type:.+}")
    @ResponseBody
    public EnvelopeResponse<String> batchUpdateProject(@PathVariable("sid_type") String sidType,
            @RequestParam("project") String project, @RequestBody Map<String, List<AclTCRRequest>> requests)
            throws IOException {
        checkProjectName(project);
        Preconditions.checkState(sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP));
        AclPermissionUtil.checkAclUpdatable(project, aclTCRService.getCurrentUserGroups());
        for (Map.Entry<String, List<AclTCRRequest>> entry : requests.entrySet()) {
            mergeSidAclTCR(project, entry.getKey(), false, entry.getValue());
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectAcl With New Foramt", tags = {
            "MID" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/{sid_type:.+}/{sid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectV2(@PathVariable("sid_type") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestBody List<AclTCRRequest> requests) throws IOException {
        checkProjectName(project);
        AclPermissionUtil.checkAclUpdatable(project, aclTCRService.getCurrentUserGroups());
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            mergeSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            mergeSidAclTCR(project, sid, false, requests);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }
}
