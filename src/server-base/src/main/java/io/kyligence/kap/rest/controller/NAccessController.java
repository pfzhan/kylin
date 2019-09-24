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
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.service.AclTCRService;

@Controller
@RequestMapping(value = "/access")
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

    private static final Pattern sidPattern = Pattern.compile("^[a-zA-Z0-9_]*$");
    private static final Message msg = MsgPicker.getMsg();

    /**
     * Get current user's permission in the project
     */
    @RequestMapping(value = "/permission/project_permission", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> getUserPermissionInPrj(
            @RequestParam(value = "project", required = true) String project) {
        String grantedPermission = accessService.getUserPermissionInPrj(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, grantedPermission, "");
    }

    @RequestMapping(value = "/available/{sidType}/{uuid}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAvailableSids(@PathVariable("uuid") String uuid, @PathVariable("sidType") String sidType,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        Acl acl = accessService.getAcl(ae);
        List<String> oriList = Lists.newArrayList();
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            oriList.addAll(getAllUserNames());
            List<String> users = accessService.getAllAclSids(acl, MetadataConstants.TYPE_USER);
            oriList.removeAll(users);
        } else {
            oriList.addAll(userGroupService.listAllAuthorities(project));
            List<String> groups = accessService.getAllAclSids(acl, MetadataConstants.TYPE_GROUP);
            oriList.removeAll(groups);
        }

        List<String> sids = PagingUtil.getIdentifierAfterFuzzyMatching(nameSeg, isCaseSensitive, oriList);
        List<String> subList = PagingUtil.cutPage(sids, pageOffset, pageSize);
        HashMap<String, Object> data = new HashMap<>();

        data.put("sids", subList);
        data.put("size", sids.size());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAccessEntities(@PathVariable("type") String type, @PathVariable("uuid") String uuid,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Acl acl = accessService.getAcl(ae);
        List<AccessEntryResponse> resultsAfterFuzzyMatching = this.accessService.generateAceResponsesByFuzzMatching(acl,
                nameSeg, isCaseSensitive);
        List<AccessEntryResponse> sublist = PagingUtil.cutPage(resultsAfterFuzzyMatching, pageOffset, pageSize);

        HashMap<String, Object> data = new HashMap<>();
        data.put("sids", sublist);
        data.put("size", resultsAfterFuzzyMatching.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Grant a new access on a domain object to a user/role
     */
    @RequestMapping(value = "/{entityType}/{uuid}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse grant(@PathVariable("entityType") String entityType, @PathVariable("uuid") String uuid,
            @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.updateAclTCR(uuid, Lists.newArrayList(accessRequest));
        }
        accessService.grant(entityType, uuid, accessRequest.getSid(), accessRequest.isPrincipal(),
                accessRequest.getPermission());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/batch/{entityType}/{uuid}", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse batchGrant(@PathVariable("entityType") String entityType, @PathVariable("uuid") String uuid,
            @RequestBody List<AccessRequest> requests) throws IOException {
        checkSid(requests);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.updateAclTCR(uuid, requests);
        }
        accessService.batchGrant(requests, entityType, uuid);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    /**
     * Update a access on a domain object
     *
     * @param accessRequest
     */

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateAcl(@PathVariable("type") String type, @PathVariable("uuid") String uuid,
            @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
        if (Objects.isNull(permission)) {
            throw new BadRequestException("Operation failed, unknown permission:" + accessRequest.getPermission());
        }
        accessService.update(ae, accessRequest.getAccessEntryId(), permission);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    /**
     * Revoke access on a domain object from a user/role
     *
     * @param accessRequest
     */

    @RequestMapping(value = "/{entityType}/{uuid}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse revokeAcl(@PathVariable("entityType") String entityType, @PathVariable("uuid") String uuid,
            AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.revoke(ae, accessRequest.getAccessEntryId());
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.revokeAclTCR(uuid, accessRequest.getSid(), accessRequest.isPrincipal());
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    private List<String> getAllUserNames() throws IOException {
        return userService.listUsers().stream().map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    private void checkSid(AccessRequest request) throws IOException {
        checkSid(Lists.newArrayList(request));
    }

    private void checkSid(List<AccessRequest> requests) throws IOException {
        if (CollectionUtils.isEmpty(requests)) {
            return;
        }
        for (AccessRequest r : requests) {
            checkSid(r.getSid(), r.isPrincipal());
        }
    }

    private void checkSid(String sid, boolean principal) throws IOException {
        if (StringUtils.isEmpty(sid)) {
            throw new BadRequestException(msg.getEMPTY_SID());
        }
        if (!sidPattern.matcher(sid).matches()) {
            throw new BadRequestException(msg.getINVALID_SID());
        }

        if (principal && !userService.userExists(sid)) {
            throw new BadRequestException("Operation failed, user:" + sid + " not exists, please add it first.");
        }
        if (!principal && !userGroupService.exists(sid)) {
            throw new BadRequestException("Operation failed, group:" + sid + " not exists, please add it first.");
        }
    }

}
