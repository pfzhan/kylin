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

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/admin", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NAdminController extends NBasicController {

    @ApiOperation(value = "getPublicConfig", tags = { "MID" }, notes = "Update Param: project_name")
    @GetMapping(value = "/public_config")
    @ResponseBody
    public EnvelopeResponse<String> getPublicConfig() throws IOException {

        final String whiteListProperties = KylinConfig.getInstanceFromEnv().getPropertiesWhiteList();

        Collection<String> propertyKeys = Lists.newArrayList();
        if (StringUtils.isNotEmpty(whiteListProperties)) {
            propertyKeys.addAll(Arrays.asList(whiteListProperties.split(",")));
        }

        // add KAP specific
        propertyKeys.add("kylin.env.smart-mode-enabled");
        propertyKeys.add("kylin.source.load-hive-tablename-enabled");
        propertyKeys.add("kylin.kerberos.project-level-enabled");
        propertyKeys.add("kylin.web.stack-trace.enabled");
        propertyKeys.add("kylin.metadata.random-admin-password.enabled");
        propertyKeys.add("kylin.model.recommendation-page-size");
        propertyKeys.add("kylin.model.dimension-measure-name.max-length");
        propertyKeys.add("kylin.favorite.import-sql-max-size");
        propertyKeys.add("kylin.model.suggest-model-sql-limit");
        propertyKeys.add("kylin.query.query-history-download-max-size");
        propertyKeys.add("kylin.streaming.enabled");
        propertyKeys.add("kylin.model.measure-name-check-enabled");
        propertyKeys.add("kylin.security.remove-ldap-custom-security-limit-enabled");

        // add second storage
        if (StringUtils.isNotEmpty(KylinConfig.getInstanceFromEnv().getSecondStorage())) {
            propertyKeys.add("kylin.second-storage.class");
        }

        if (!KylinConfig.getInstanceFromEnv().isAllowedNonAdminGenerateQueryDiagPackage()) {
            propertyKeys.add("kylin.security.allow-non-admin-generate-query-diag-package");
        }

        final String config = KylinConfig.getInstanceFromEnv().exportToString(propertyKeys) + addPropertyInMetadata();

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, config, "");
    }

    @ApiOperation(value = "health APIs", tags = { "SM" })
    @GetMapping(value = "/instance_info")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getInstanceConfig() {

        Map<String, String> data = Maps.newHashMap();

        ZoneId zoneId = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).toZoneId();
        data.put("instance.timezone", zoneId.toString());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    private String addPropertyInMetadata() {
        Properties properties = new Properties();
        ResourceGroupManager manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        properties.put("resource_group_enabled", manager.isResourceGroupEnabled());
        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }

}
