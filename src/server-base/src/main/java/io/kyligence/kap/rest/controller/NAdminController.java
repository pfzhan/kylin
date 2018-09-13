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
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

@Controller
@RequestMapping(value = "/admin")
public class NAdminController extends NBasicController {

    @RequestMapping(value = "/public_config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getPublicConfig(@RequestParam(value = "projectName", required = false) String projectName)
            throws IOException {

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

        final String config = KylinConfig.getInstanceFromEnv().exportToString(propertyKeys);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, config, "");
    }

}
