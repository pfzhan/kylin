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

import com.google.common.collect.Maps;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.service.KapQueryService;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.Map;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

@RestController
@RequestMapping(value = "/api/query")
public class NQueryControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("kapQueryService")
    private KapQueryService queryService;

    @Deprecated
    @PostMapping(value = "", produces = { "application/json" })
    @ResponseBody
    public SQLResponse query4JDBC(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        return queryService.doQueryWithCache(sqlRequest, false);
    }

    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<SQLResponse> query(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest, false);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, sqlResponse, "");
    }

    @PostMapping(value = "/prestate", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<SQLResponse> prepareQuery(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, queryService.doQueryWithCache(sqlRequest, false), "");
    }
}
