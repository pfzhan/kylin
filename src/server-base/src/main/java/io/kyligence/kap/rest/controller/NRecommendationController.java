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

import java.util.List;

import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.OptRecRequest;
import io.kyligence.kap.rest.response.OptRecDetailResponse;
import io.kyligence.kap.rest.response.OptRecLayoutsResponse;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.OptimizeRecommendationService;
import io.kyligence.kap.rest.service.RawRecService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/recommendations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NRecommendationController extends NBasicController {

    private static final String MODEL_ID = "modelId";

    @Autowired
    @Qualifier("optRecService")
    private OptRecService optRecService;

    @Autowired
    private RawRecService rawRecService;

    @Autowired
    @Qualifier("optimizeRecommendationService")
    private OptimizeRecommendationService optimizeRecommendationService;

    @ApiOperation(value = "approveOptimizeRecommendations", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<String> approveOptimizeRecommendations(@PathVariable("model") String modelId,
            @RequestBody OptRecRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        request.setModelId(modelId);
        optRecService.approve(request.getProject(), request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "validateOptimizeRecommendations", notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/validation")
    @ResponseBody
    public EnvelopeResponse<OptRecDetailResponse> validateOptimizeRecommendations(@PathVariable("model") String modelId,
            @RequestBody OptRecRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                optRecService.getOptRecDetail(request.getProject(), modelId, request.getLayoutIdsToAdd()), "");
    }

    @ApiOperation(value = "cleanOptimizeRecommendations", notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}/all")
    @ResponseBody
    public EnvelopeResponse<String> cleanOptimizeRecommendations(@PathVariable("model") String modelId,
            @PathVariable("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        optRecService.clean(project, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "removeOptimizeRecommendationsV2", notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteOptimizeRecommendationsV2(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "layouts_to_remove", required = false) List<Integer> layoutsToRemove,
            @RequestParam(value = "layouts_to_add", required = false) List<Integer> layoutsToAdd) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        val request = new OptRecRequest();
        request.setModelId(modelId);
        request.setProject(project);
        if (layoutsToRemove != null)
            request.setLayoutIdsToRemove(layoutsToRemove);
        if (layoutsToAdd != null)
            request.setLayoutIdsToAdd(layoutsToAdd);

        optRecService.delete(request.getProject(), request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getOptimizeRecommendations", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<OptRecLayoutsResponse> getOptimizeRecommendations(
            @PathVariable(value = "model") String modelId, @RequestParam(value = "project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                optRecService.getOptRecLayoutsResponse(project, modelId), "");
    }

    @ApiOperation(value = "getOptimizeRecommendationDetail", notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/{item_id:.+}")
    @ResponseBody
    public EnvelopeResponse<OptRecDetailResponse> getOptimizeRecommendations(
            @PathVariable(value = "model") String modelId, @PathVariable(value = "item_id") Integer itemId,
            @RequestParam(value = "project") String project, @RequestParam(value = "is_add", defaultValue = "true") boolean isAdd) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                optRecService.getSingleOptRecDetail(project, modelId, itemId, isAdd), "");
    }

    @ApiOperation(value = "accelerate query history and select topn", notes = "Add URL: {model}")
    @PutMapping(value = "/acceleration")
    @ResponseBody
    public EnvelopeResponse<String> accelerate(@RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        rawRecService.accelerate(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}
