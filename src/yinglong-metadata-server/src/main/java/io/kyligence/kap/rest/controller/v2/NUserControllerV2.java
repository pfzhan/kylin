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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NUserController;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping("/api")
public class NUserControllerV2 extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NUserControllerV2.class);

    @Autowired
    private NUserController nUserController;

    @ApiOperation(value = "listAllUsers", tags = { "MID" })
    @GetMapping(value = "/kap/user/users", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse listAllUsers(@RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        EnvelopeResponse<DataResult<List<ManagedUser>>> dataResult = nUserController.listAllUsers(nameSeg,
                isCaseSensitive, pageOffset, pageSize);
        Map<String, Object> result = Maps.newHashMap();
        result.put("users", dataResult.getData().getValue());
        return new EnvelopeResponse<>(dataResult.getCode(), result, dataResult.getMsg());
    }

    @ApiOperation(value = "authenticate", tags = { "MID" })
    @PostMapping(value = "/user/authentication", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticate() {
        EnvelopeResponse<UserDetails> response = authenticatedUser();
        logger.debug("User login: {}", response.getData());
        return response;
    }

    @ApiOperation(value = "authenticatedUser", tags = { "MID" })
    @GetMapping(value = "/user/authentication", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails data = null;
        val msg = MsgPicker.getMsg();
        if (authentication == null) {
            throw new UnauthorizedException(USER_AUTH_INFO_NOTFOUND);
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            data = (UserDetails) authentication.getPrincipal();
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
        }

        if (authentication.getDetails() instanceof UserDetails) {
            data = (UserDetails) authentication.getDetails();
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
        }

        throw new UnauthorizedException(USER_AUTH_INFO_NOTFOUND);
    }
}
