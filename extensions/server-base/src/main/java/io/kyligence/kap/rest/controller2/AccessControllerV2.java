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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.PagingUtil;
import io.kyligence.kap.rest.service.UserGroupService;
import io.kyligence.kap.rest.util.ACLOperationUtil;

/**
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/access")
public class AccessControllerV2 extends BasicController {

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    private UserGroupService userGroupService;

    /**
     * Get current user's permission in the project
     */
    @RequestMapping(value = "/user/permission/{project}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> getUserPermissionInPrj(@PathVariable("project") String project) {
        String grantedPermission = accessService.getUserPermissionInPrj(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, grantedPermission, "");
    }

    /**
     * Get users/groups that can be added to project ACL.
     */
    @RequestMapping(value = "/available/{sidType}/{uuid}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAvailableSids(@PathVariable String uuid, @PathVariable String sidType,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        Acl acl = accessService.getAcl(ae);
        final String GROUP = "group";
        final String USER = "user";
        List<String> oriList = new ArrayList<>();
        if (sidType.equalsIgnoreCase(USER)) {
            List<ManagedUser> allUsers = userService.listUsers();
            List<String> users = accessService.getAllAclSids(acl, USER);
            for (ManagedUser u : allUsers) {
                if (!users.contains(u.getUsername())) {
                    oriList.add(u.getUsername());
                }
            }
        }
        if (sidType.equalsIgnoreCase(GROUP)) {
            oriList.addAll(userGroupService.listAllAuthorities(project));
            List<String> groups = accessService.getAllAclSids(acl, GROUP);
            oriList.removeAll(groups);
        }

        List<String> sids = PagingUtil.getIdentifierAfterFuzzyMatching(nameSeg, isCaseSensitive, oriList);
        List<String> subList = PagingUtil.cutPage(sids, pageOffset, pageSize);
        HashMap<String, Object> data = new HashMap<>();

        data.put("sids", subList);
        data.put("size", sids.size());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Get access entry list of a domain object
     *
     * @param uuid
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAccessEntitiesV2(@PathVariable String type, @PathVariable String uuid,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Acl acl = accessService.getAcl(ae);
        List<AccessEntryResponse> resultsAfterFuzzyMatching = this.accessService.generateAceResponsesByFuzzMatching(acl, nameSeg, isCaseSensitive);
        List<AccessEntryResponse> sublist = PagingUtil.cutPage(resultsAfterFuzzyMatching, pageOffset, pageSize);

        HashMap<String, Object> data = new HashMap<>();
        data.put("sids", sublist);
        data.put("size", resultsAfterFuzzyMatching.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Grant a new access on a domain object to a user/role
     * 
     * @param accessRequest
     */

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse grantV2(@PathVariable String type, @PathVariable String uuid,
            @RequestBody AccessRequest accessRequest) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Sid sid = accessService.getSid(accessRequest.getSid(), accessRequest.isPrincipal());
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
        accessService.grant(ae, permission, sid);

        if (AclEntityType.CUBE_INSTANCE.equals(type)) {
            CubeInstance instance = cubeService.getCubeManager().getCubeByUuid(uuid);
            if (instance != null) {
                AclEntity model = accessService.getAclEntity(AclEntityType.DATA_MODEL_DESC, instance.getModel().getUuid());
                // FIXME: should not always grant
                accessService.grant(model, permission, sid);
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    /**
     * Update a access on a domain object
     * 
     * @param accessRequest
     */

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateV2(@PathVariable String type, @PathVariable String uuid,
            @RequestBody AccessRequest accessRequest) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
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
    public EnvelopeResponse revokeV2(@PathVariable String entityType, @PathVariable String uuid,
            AccessRequest accessRequest) throws IOException {
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        if (accessRequest.isPrincipal()) {
            revokeLowLevelACL(entityType, uuid, accessRequest.getSid(), MetadataConstants.TYPE_USER);
        } else {
            revokeLowLevelACL(entityType, uuid, accessRequest.getSid(), MetadataConstants.TYPE_GROUP);
        }
        accessService.revoke(ae, accessRequest.getAccessEntryId());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    private void revokeLowLevelACL(String entityType, String uuid, String name, String type) throws IOException {
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            String prj = projectService.getProjectManager().getPrjByUuid(uuid).getName();
            ACLOperationUtil.delLowLevelACLByPrj(prj, name, type);
        }
    }

    /**
     * @param accessService
     */
    public void setAccessService(AccessService accessService) {
        this.accessService = accessService;
    }

}
