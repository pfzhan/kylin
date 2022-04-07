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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.controller.NBasicController;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/user_group")
public class NUserGroupControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @ApiOperation(value = "getUsersWithGroup", tags = { "MID" })
    @GetMapping(value = "/usersWithGroup", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<Map<String, Object>> getUsersWithGroup(
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "userGroupName", required = false, defaultValue = "") String userGroupName)
            throws IOException {
        List<Pair<String, Set<String>>> usersWithGroup = new ArrayList<>();
        List<String> groups = userGroupService.getAuthoritiesFilterByGroupName(userGroupName);

        List<String> subList = PagingUtil.cutPage(groups, pageOffset, pageSize);
        for (String group : subList) {
            Set<String> groupMembers = new TreeSet<>();
            for (ManagedUser user : userGroupService.getGroupMembersByName(group)) {
                groupMembers.add(user.getUsername());
            }
            usersWithGroup.add(Pair.newPair(group, groupMembers));
        }
        Map<String, Object> result = new HashMap<>();
        result.put("usersWithGroup", usersWithGroup);
        result.put("size", groups.size());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "get users with group");
    }

}
