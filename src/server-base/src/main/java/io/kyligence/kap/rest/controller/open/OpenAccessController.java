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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.UNAUTHORIZED_ENTITY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ProjectPermissionResponse;
import org.apache.kylin.rest.response.SidPermissionWithAclResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.ExternalAclProvider;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.request.BatchProjectPermissionRequest;
import io.kyligence.kap.rest.request.ProjectPermissionRequest;
import io.kyligence.kap.rest.service.AclTCRService;

@Controller
@RequestMapping(value = "/api/access", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenAccessController extends NBasicController {
    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @GetMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ProjectPermissionResponse>>> getProjectAccessPermissions(
        @RequestParam("project") String project,
        @RequestParam(value = "name", required = false) String name,
        @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
        @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
        @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        checkProjectName(project);
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, getProjectUuid(project));
        List<AccessEntryResponse> aeResponses = accessService.generateAceResponsesByFuzzMatching(ae, name, isCaseSensitive);
        List<ProjectPermissionResponse> permissionResponses = convertAceResponseToProjectPermissionResponse(aeResponses);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(permissionResponses, pageOffset, pageSize),
                "");
    }

    @PostMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<String> grantProjectPermission(
        @RequestBody BatchProjectPermissionRequest permissionRequest) throws IOException {
        checkProjectName(permissionRequest.getProject());
        checkType(permissionRequest.getType());
        checkNames(permissionRequest.getNames());
        ExternalAclProvider.checkExternalPermission(permissionRequest.getPermission());

        List<AccessRequest> accessRequests = convertBatchPermissionRequestToAccessRequests(permissionRequest);
        accessService.checkAccessRequestList(accessRequests);

        String projectUuid = getProjectUuid(permissionRequest.getProject());
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        accessService.batchGrant(accessRequests, ae);
        aclTCRService.updateAclTCR(projectUuid, accessRequests);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PutMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectPermission(
            @RequestBody ProjectPermissionRequest permissionRequest) throws IOException {
        checkProjectName(permissionRequest.getProject());
        checkType(permissionRequest.getType());
        checkName(permissionRequest.getName());
        ExternalAclProvider.checkExternalPermission(permissionRequest.getPermission());

        AccessRequest accessRequest = convertPermissionRequestToAccessRequest(permissionRequest);
        accessService.checkAccessRequestList(Lists.newArrayList(accessRequest));

        String projectUuid = getProjectUuid(permissionRequest.getProject());
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        accessService.grant(ae, accessRequest.getSid(), accessRequest.isPrincipal(), accessRequest.getPermission());
        aclTCRService.updateAclTCR(projectUuid, Lists.newArrayList(accessRequest));

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @DeleteMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<String> revokeProjectPermission(
        @RequestParam("project") String project,
        @RequestParam("type") String type,
        @RequestParam("name") String name) throws IOException {
        checkProjectName(project);
        checkType(type);
        checkSidExists(type, name);
        checkSidGranted(project, name);

        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(type);
        if (principal) {
            accessService.checkGlobalAdmin(name);
        }
        String projectUuid = getProjectUuid(project);
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        accessService.revokeWithSid(ae, name, principal);
        aclTCRService.revokeAclTCR(projectUuid, name, principal);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/acls")
    @ResponseBody
    public EnvelopeResponse<List<SidPermissionWithAclResponse>> getUserOrGroupAclPermissions(
            @RequestParam("type") String type,
            @RequestParam(value = "name") String name,
            @RequestParam(value = "project", required = false) String project) throws IOException {
        if (StringUtils.isNotBlank(project)) {
            checkProjectName(project);
        }
        checkType(type);
        checkSidExists(type, name);

        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(type);

        List<String> projects = new ArrayList<>();
        if (StringUtils.isBlank(project)) {
            projects = accessService.getGrantedProjectsOfUserOrGroup(name, principal);
        } else {
            projects.add(project);
        }

        List<SidPermissionWithAclResponse> response = accessService.getUserOrGroupAclPermissions(projects, name, principal);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    private void checkSidExists(String type, String name) throws IOException {
        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(type);
        accessService.checkSid(name, principal);
    }

    private void checkSidGranted(String project, String name) throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, getProjectUuid(project));
        List<AccessEntryResponse> aeResponses = accessService.generateAceResponsesByFuzzMatching(ae, name, false);
        if (CollectionUtils.isEmpty(aeResponses)) {
            throw new KylinException(UNAUTHORIZED_ENTITY, MsgPicker.getMsg().getUNAUTHORIZED_SID());
        }
    }

    private void checkType(String type) {
        if (MetadataConstants.TYPE_USER.equalsIgnoreCase(type) || MetadataConstants.TYPE_GROUP.equalsIgnoreCase(type)) {
            return;
        }
        throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_PARAMETER_TYPE());
    }

    private void checkNames(List<String> names) {
        if (CollectionUtils.isEmpty(names) || names.stream().anyMatch(StringUtils::isBlank)) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEMPTY_SID());
        }
    }

    private void checkName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEMPTY_SID());
        }
    }

    private String getProjectUuid(String project) {
        checkProjectName(project);
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(project);
        return prjInstance.getUuid();
    }

    private AccessRequest convertPermissionRequestToAccessRequest(ProjectPermissionRequest permissionRequest) {
        AccessRequest accessRequest = new AccessRequest();

        accessRequest.setPrincipal(MetadataConstants.TYPE_USER.equalsIgnoreCase(permissionRequest.getType()));
        accessRequest.setSid(permissionRequest.getName());
        String permission = AclPermissionEnum.convertToAclPermission(permissionRequest.getPermission().toUpperCase());
        accessRequest.setPermission(permission);

        return accessRequest;
    }

    private List<AccessRequest> convertBatchPermissionRequestToAccessRequests(BatchProjectPermissionRequest permissionRequest) {
        List<AccessRequest> accessRequests = new ArrayList<>();
        String type = permissionRequest.getType();
        String externalPermission = permissionRequest.getPermission().toUpperCase();
        String aclPermission = AclPermissionEnum.convertToAclPermission(externalPermission);

        List<String> names = permissionRequest.getNames();
        for (String name : names) {
            AccessRequest accessRequest = new AccessRequest();
            accessRequest.setPermission(aclPermission);
            accessRequest.setPrincipal(MetadataConstants.TYPE_USER.equalsIgnoreCase(type));
            accessRequest.setSid(name);
            accessRequests.add(accessRequest);
        }
        return accessRequests;
    }

    private List<ProjectPermissionResponse> convertAceResponseToProjectPermissionResponse(List<AccessEntryResponse> aclResponseList) {
        List<ProjectPermissionResponse> responseList = new ArrayList<>();
        for (AccessEntryResponse aclResponse : aclResponseList) {
            String type = "";
            String userOrGroupName = "";
            Sid sid = aclResponse.getSid();
            if (sid instanceof PrincipalSid) {
                type = MetadataConstants.TYPE_USER;
                userOrGroupName = ((PrincipalSid) sid).getPrincipal();
            } else if (sid instanceof GrantedAuthoritySid) {
                type = MetadataConstants.TYPE_GROUP;
                userOrGroupName = ((GrantedAuthoritySid) sid).getGrantedAuthority();
            }
            String externalPermission = ExternalAclProvider.convertToExternalPermission(aclResponse.getPermission());
            responseList.add(new ProjectPermissionResponse(type, userOrGroupName, externalPermission));
        }
        return responseList;
    }
}
