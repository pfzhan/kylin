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
import java.util.Arrays;
import java.util.Collection;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.request.UpdateConfigRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.AdminService;
import org.apache.kylin.rest.service.CubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Admin Controller is defined as Restful API entrance for UI.
 * 
 * @author jianliu
 * 
 */
@Controller
@RequestMapping(value = "/admin")
public class AdminControllerV2 extends BasicController {

    @Autowired
    @Qualifier("adminService")
    private AdminService adminService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeMgmtService;

    @RequestMapping(value = "/env", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getEnvV2() throws ConfigurationException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, adminService.getEnv(), "");
    }

    @RequestMapping(value = "/config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getConfigV2() throws IOException {

        String config = KylinConfig.getInstanceFromEnv().exportAllToString();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, config, "");
    }

    @RequestMapping(value = "/public_config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getPublicConfigV2() throws IOException {

        final String whiteListProperties = KylinConfig.getInstanceFromEnv().getPropertiesWhiteList();

        Collection<String> propertyKeys = Lists.newArrayList();
        if (StringUtils.isNotEmpty(whiteListProperties)) {
            propertyKeys.addAll(Arrays.asList(whiteListProperties.split(",")));
        }

        // add KAP specific
        propertyKeys.add("kap.kyaccount.username");
        propertyKeys.add("kap.license.statement");
        propertyKeys.add("kap.web.hide-feature.raw-measure");
        propertyKeys.add("kap.web.hide-feature.extendedcolumn-measure");
        propertyKeys.add("kap.web.hide-feature.limited-lookup");
        propertyKeys.add("kap.smart.conf.aggGroup.strategy");

        final String config = KylinConfig.getInstanceFromEnv().exportToString(propertyKeys);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, config, "");
    }

    @RequestMapping(value = "/metrics/cubes", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cubeMetricsV2(MetricsRequest request) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeMgmtService.calculateMetrics(request), "");
    }

    @RequestMapping(value = "/storage", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void cleanupStorageV2() {

        adminService.cleanupStorage();
    }

    @RequestMapping(value = "/config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateKylinConfigV2(@RequestBody UpdateConfigRequest updateConfigRequest) {

        KylinConfig.getInstanceFromEnv().setProperty(updateConfigRequest.getKey(), updateConfigRequest.getValue());
    }

    public void setAdminService(AdminService adminService) {
        this.adminService = adminService;
    }

    public void setCubeMgmtService(CubeService cubeMgmtService) {
        this.cubeMgmtService = cubeMgmtService;
    }

}
