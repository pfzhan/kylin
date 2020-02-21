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

package io.kyligence.kap.rest.controller.v3;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V3_JSON;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.AclPermissionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.rest.controller.NIndexPlanController;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.controller.NProjectController;
import io.kyligence.kap.rest.controller.NSystemController;
import io.kyligence.kap.rest.controller.NUserController;
import lombok.val;

@Deprecated
@RestController
@RequestMapping("/api")
public class KICompatibleController {

    @Autowired
    NModelController modelController;

    @Autowired
    NIndexPlanController indexPlanController;

    @Autowired
    NProjectController projectController;

    @Autowired
    NSystemController systemController;

    @Autowired
    NUserController userController;

    @GetMapping(value = "/models", produces = { HTTP_VND_APACHE_KYLIN_V3_JSON })
    public EnvelopeResponse getModels(
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse,
            @RequestParam(value = "project") String project) {
        val result = modelController.getModels(null, true, project, null, null, null, offset, limit, sortBy, reverse);
        Map<String, Object> response = Maps.newHashMap();
        response.put("models", result.getData().getValue());
        response.put("size", result.getData().getTotalSize());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @GetMapping(value = "/index_plans/rule", produces = { HTTP_VND_APACHE_KYLIN_V3_JSON })
    public EnvelopeResponse<NRuleBasedIndex> getRule(@RequestParam("project") String project,
            @RequestParam("model") String modelId) {
        return indexPlanController.getRule(project, modelId);
    }

    @GetMapping(value = "/projects", produces = { HTTP_VND_APACHE_KYLIN_V3_JSON })
    public EnvelopeResponse getProjects(
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer size) {
        val result = projectController.getProjects(null, offset, size, false, AclPermissionType.READ);
        Map<String, Object> response = Maps.newHashMap();
        response.put("projects", result.getData().getValue());
        response.put("size", result.getData().getTotalSize());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @GetMapping(value = "/system/license", produces = { HTTP_VND_APACHE_KYLIN_V3_JSON })
    public EnvelopeResponse getLicense() {
        return systemController.listLicense();
    }

    @GetMapping(value = "/user/authentication", produces = { HTTP_VND_APACHE_KYLIN_V3_JSON })
    public EnvelopeResponse auth() {
        return userController.authenticatedUser();
    }

    @GetMapping(value = "/user", produces = { HTTP_VND_APACHE_KYLIN_V3_JSON })
    public EnvelopeResponse getUser(
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        val result = userController.listAllUsers(null, null, false, pageOffset, pageSize);
        Map<String, Object> response = Maps.newHashMap();
        response.put("users", result.getData().getValue());
        response.put("size", result.getData().getTotalSize());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

}
