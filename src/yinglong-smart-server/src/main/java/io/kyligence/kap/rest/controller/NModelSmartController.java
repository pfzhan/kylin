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
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_MODEL;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.model.exception.LookupTableException;
import io.kyligence.kap.rest.request.ModelSuggestionRequest;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.smart.AbstractContext;
import io.swagger.annotations.ApiOperation;
import lombok.extern.log4j.Log4j;

@Log4j
@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NModelSmartController extends NBasicController {

    @Autowired
    private ModelSmartService modelSmartService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @ApiOperation(value = "suggestModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/suggest_model")
    @ResponseBody
    public EnvelopeResponse<SuggestionResponse> suggestModel(@RequestBody SqlAccelerateRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        AbstractContext proposeContext = modelSmartService.suggestModel(request.getProject(), request.getSqls(),
                request.getReuseExistedModel(), true);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                modelSmartService.buildModelSuggestionResponse(proposeContext), "");
    }

    @ApiOperation(value = "checkIfCanAnsweredByExistedModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/can_answered_by_existed_model")
    @ResponseBody
    public EnvelopeResponse<Boolean> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                modelSmartService.couldAnsweredByExistedModel(request.getProject(), request.getSqls()), "");
    }

    @ApiOperation(value = "suggestModel", tags = { "AI" }, notes = "")
    @PostMapping(value = "/model_recommendation")
    @ResponseBody
    public EnvelopeResponse<String> approveSuggestModel(@RequestBody ModelSuggestionRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        try {
            request.getNewModels().forEach(req -> {
                req.setWithModelOnline(request.isWithModelOnline());
                req.setWithEmptySegment(request.isWithEmptySegment());
            });
            modelService.batchCreateModel(request.getProject(), request.getNewModels(), request.getReusedModels());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_CREATE_MODEL, e.getMessage(), e);
        }
    }
}
