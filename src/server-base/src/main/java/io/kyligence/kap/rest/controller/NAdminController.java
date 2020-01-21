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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/admin", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NAdminController extends NBasicController {

    @ApiOperation(value = "getPublicConfig (update)", notes = "Update Param: project_name")
    @GetMapping(value = "/public_config")
    @ResponseBody
    public EnvelopeResponse<String> getPublicConfig() throws IOException {

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
        propertyKeys.add("kap.canary.default-canaries-period-min");
        propertyKeys.add("kylin.env.smart-mode-enabled");
        propertyKeys.add("kap.table.load-hive-tablename-enabled");

        final String config = KylinConfig.getInstanceFromEnv().exportToString(propertyKeys);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, config, "");
    }

    @GetMapping(value = "/instance_info")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getInstanceConfig() {

        Map<String, String> data = Maps.newHashMap();

        ZoneId zoneId = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).toZoneId();
        data.put("instance.timezone", zoneId.toString());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "");
    }

}
