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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.rest.request.SQLValidateRequest;
import io.kyligence.kap.rest.response.SQLParserResponse;
import io.kyligence.kap.rest.response.SQLValidateResponse;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api/query/favorite_queries", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class FavoriteQueryController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger("query");
    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "importSqls (response)", tags = { "AI" }, notes = "sql_advices")
    @PostMapping(value = "/sql_files")
    @ResponseBody
    public EnvelopeResponse<SQLParserResponse> importSqls(@RequestParam("project") String project,
            @RequestParam("files") MultipartFile[] files) {
        checkProjectName(project);
        checkProjectUnmodifiable(project);
        SQLParserResponse data = favoriteRuleService.importSqls(files, project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    @ApiOperation(value = "sqlValidate", tags = { "AI" }, notes = "Update Response: incapable_reason, sql_advices")
    @PutMapping(value = "/sql_validation")
    @ResponseBody
    public EnvelopeResponse<SQLValidateResponse> sqlValidate(@RequestBody SQLValidateRequest request) {
        checkProjectName(request.getProject());
        checkProjectUnmodifiable(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                favoriteRuleService.sqlValidate(request.getProject(), request.getSql()), "");
    }
}
