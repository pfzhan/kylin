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
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.model.Permission;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.request.BatchAccessRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.ProjectService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/access", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NAccessController extends NBasicController {

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    /**
     * Get current user's permission in the project
     */
    @GetMapping(value = "/permission/project_permission", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> getUserPermissionInPrj(@RequestParam(value = "project") String project)
            throws IOException {
        checkProjectName(project);
        List<String> groups = accessService.getGroupsOfCurrentUser();
        String permission = groups.contains(ROLE_ADMIN) ? "GLOBAL_ADMIN"
                : AclPermissionEnum.convertToAclPermission(accessService.getCurrentUserPermissionInProject(project));

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, permission, "");
    }

    @ApiOperation(value = "getAvailableSids", notes = "Update Param: sid_type, is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/available/{sid_type:.+}/{uuid:.+}")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<String>>> getAvailableSids(@PathVariable("uuid") String uuid,
            @PathVariable("sid_type") String sidType, @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        List<String> whole = Lists.newArrayList();
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            whole.addAll(getAllUserNames());
            List<String> users = accessService.getAllAclSids(ae, MetadataConstants.TYPE_USER);
            whole.removeAll(users);
            whole.removeAll(userService.listAdminUsers());
        } else {
            whole.addAll(getAllUserGroups());
            List<String> groups = accessService.getAllAclSids(ae, MetadataConstants.TYPE_GROUP);
            whole.removeAll(groups);
            whole.remove(ROLE_ADMIN);
        }

        List<String> matchedSids = PagingUtil.getIdentifierAfterFuzzyMatching(nameSeg, isCaseSensitive, whole);
        List<String> subList = PagingUtil.cutPage(matchedSids, pageOffset, pageSize);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(subList, matchedSids), "");
    }

    @GetMapping(value = "/available/{entity_type:.+}")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<String>>> getAvailableUsers(@PathVariable("entity_type") String entityType,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "model", required = false) String modelId,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        checkProjectName(project);

        List<String> whole = Lists.newArrayList();
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            whole.addAll(accessService.getProjectAdminUsers(project));
            whole.remove(getProject(project).getOwner());
        } else {
            checkRequiredArg("model", modelId);
            whole.addAll(accessService.getProjectManagementUsers(project));

            NDataModel model = projectService.getDataModelManager(project).getDataModelDesc(modelId);
            if (Objects.isNull(model) || model.isBroken()) {
                throw new KylinException(MODEL_NOT_EXIST,
                        "Model " + modelId + "does not exist or broken in project " + project);
            }
            whole.remove(model.getOwner());
        }

        List<String> matchedUsers = PagingUtil.getIdentifierAfterFuzzyMatching(nameSeg, isCaseSensitive, whole);
        List<String> subList = PagingUtil.cutPage(matchedUsers, pageOffset, pageSize);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(subList, matchedUsers), "");
    }

    @ApiOperation(value = "getAccessEntities", notes = "Update Param: is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/{type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<AccessEntryResponse>>> getAccessEntities(@PathVariable("type") String type,
            @PathVariable("uuid") String uuid, @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        List<AccessEntryResponse> resultsAfterFuzzyMatching = this.accessService.generateAceResponsesByFuzzMatching(ae,
                nameSeg, isCaseSensitive);
        List<AccessEntryResponse> sublist = PagingUtil.cutPage(resultsAfterFuzzyMatching, pageOffset, pageSize);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(sublist, resultsAfterFuzzyMatching),
                "");
    }


    @ApiOperation(value = "get users and groups for project", notes = "")
    @GetMapping(value = "/{uuid:.+}/all", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, List<String>>> getProjectUsersAndGroups(@PathVariable("uuid") String uuid) {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        Map<String, List<String>> result = Maps.newHashMap();
        List<String> users = accessService.getAllAclSids(ae, MetadataConstants.TYPE_USER);
        List<String> groups = accessService.getAllAclSids(ae, MetadataConstants.TYPE_GROUP);
        result.put("user", users);
        result.put("group", groups);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    /**
     * Grant a new access on a domain object to a user/role
     */
    @ApiOperation(value = "grant", notes = "Update URL: {entity_type}; Update Body: access_entry_id")
    @PostMapping(value = "/{entity_type:.+}/{uuid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> grant(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid, @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        if (accessRequest.isPrincipal()) {
            accessService.checkGlobalAdmin(accessRequest.getSid());
        }
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.grant(ae, accessRequest.getSid(), accessRequest.isPrincipal(), accessRequest.getPermission());
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.updateAclTCR(uuid, Lists.newArrayList(accessRequest));
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "batchGrant", notes = "Update URL: {entity_type}; Update Body: access_entry_id")
    @PostMapping(value = "/batch/{entity_type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> batchGrant(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid,
            @RequestParam(value = "init_acl", required = false, defaultValue = "true") boolean initAcl,
            @RequestBody List<BatchAccessRequest> batchAccessRequests) throws IOException {
        List<AccessRequest> requests = transform(batchAccessRequests);
        accessService.checkAccessRequestList(requests);

        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.batchGrant(requests, ae);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType) && initAcl) {
            aclTCRService.updateAclTCR(uuid, requests);
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    /**
     * Update a access on a domain object
     *
     * @param accessRequest
     */
    @ApiOperation(value = "batchGrant", notes = "Update Body: access_entry_id")
    @PutMapping(value = "/{type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> updateAcl(@PathVariable("type") String type, @PathVariable("uuid") String uuid,
            @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
        if (Objects.isNull(permission)) {
            throw new KylinException(PERMISSION_DENIED,
                    "Operation failed, unknown permission:" + accessRequest.getPermission());
        }
        if (accessRequest.isPrincipal()) {
            accessService.checkGlobalAdmin(accessRequest.getSid());
        }
        accessService.update(ae, accessRequest.getAccessEntryId(), permission);
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, hasAdminProject, "");
    }

    @ApiOperation(value = "revokeAcl", notes = "URL: entity_type; Update Param: entity_type; Update Body: access_entry_id")
    @DeleteMapping(value = "/{entity_type:.+}/{uuid:.+}")
    @ResponseBody
    public EnvelopeResponse<Boolean> revokeAcl(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid, //
            @RequestParam("access_entry_id") Integer accessEntryId, //
            @RequestParam("sid") String sid, //
            @RequestParam("principal") boolean principal) throws IOException {
        accessService.checkSidNotEmpty(sid, principal);
        if (principal) {
            accessService.checkGlobalAdmin(sid);
        }
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.revoke(ae, accessEntryId);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.revokeAclTCR(uuid, sid, principal);
        }
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, hasAdminProject, "");
    }

    @ApiOperation(value = "batch revoke Acl", notes = "")
    @PostMapping(value = "/{entity_type:.+}/{uuid:.+}/deletion", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> deleteAces(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid, @RequestBody List<AccessRequest> requests) throws IOException {
        for (AccessRequest request : requests) {
            checkSid(request);
        }
        List<String> users = requests.stream().filter(AccessRequest::isPrincipal).map(AccessRequest::getSid)
                .collect(Collectors.toList());
        accessService.checkGlobalAdmin(users);
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.batchRevoke(ae, requests);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            requests.forEach(r -> aclTCRService.revokeAclTCR(uuid, r.getSid(), r.isPrincipal()));
        }
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, hasAdminProject, "");
    }

    private List<String> getAllUserNames() throws IOException {
        return userService.listUsers().stream().map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    private List<String> getAllUserGroups() throws IOException {
        return userGroupService.getAllUserGroups();
    }

    private void checkSid(AccessRequest request) throws IOException {
        accessService.checkSid(Lists.newArrayList(request));
    }

    private List<AccessRequest> transform(List<BatchAccessRequest> batchAccessRequests) {
        List<AccessRequest> result = Optional.ofNullable(batchAccessRequests).map(List::stream).orElseGet(Stream::empty)
                .map(b -> Optional.ofNullable(b.getSids()).map(List::stream).orElseGet(Stream::empty).map(sid -> {
                    AccessRequest r = new AccessRequest();
                    r.setAccessEntryId(b.getAccessEntryId());
                    r.setPermission(b.getPermission());
                    r.setPrincipal(b.isPrincipal());
                    r.setSid(sid);
                    return r;
                }).collect(Collectors.toList())).flatMap(List::stream).collect(Collectors.toList());
        Set<String> sids = Sets.newHashSet();
        result.stream().filter(r -> !sids.add(r.getSid())).findAny().ifPresent(r -> {
            throw new KylinException(DUPLICATE_USER_NAME, "Operation failed, duplicated sid: " + r.getSid());
        });
        return result;
    }

}
