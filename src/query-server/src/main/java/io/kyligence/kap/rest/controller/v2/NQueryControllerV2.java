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

import java.util.Map;

import javax.validation.Valid;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Maps;

import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.response.SQLResponseV2;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api/query")
public class NQueryControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Deprecated
    @ApiOperation(value = "query4JDBC", tags = { "QE" })
    @PostMapping(value = "", produces = { "application/json" })
    @ResponseBody
    public SQLResponse query4JDBC(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        String projectName = checkProjectName(sqlRequest.getProject());
        sqlRequest.setProject(projectName);
        return queryService.queryWithCache(sqlRequest);
    }


    @ApiOperation(value = "query", tags = { "QE" })
    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<SQLResponseV2> query(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        String projectName = checkProjectName(sqlRequest.getProject());
        sqlRequest.setProject(projectName);
        SQLResponse sqlResponse = queryService.queryWithCache(sqlRequest);
        SQLResponseV2 sqlResponseV2 = null == sqlResponse ? null : new SQLResponseV2(sqlResponse);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlResponseV2, "");
    }

    @ApiOperation(value = "prepareQuery", tags = { "QE" })
    @PostMapping(value = "/prestate", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<SQLResponse> prepareQuery(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        String projectName = checkProjectName(sqlRequest.getProject());
        sqlRequest.setProject(projectName);
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryService.queryWithCache(sqlRequest), "");
    }
}
