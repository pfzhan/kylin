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

import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.service.IndexPlanService;
import lombok.val;

import javax.validation.Valid;

@RestController
@RequestMapping(value = "/index_plans")
public class NIndexPlanController extends NBasicController {

    private static final String MODEL_ID = "modelId";

    @Autowired
    @Qualifier("indexPlanService")
    private IndexPlanService indexPlanService;

    @PutMapping(value = "/rule", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse updateRule(@RequestBody UpdateRuleBasedCuboidRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        indexPlanService.checkIndexCountWithinLimit(request);
        val response = indexPlanService.updateRuleBasedCuboid(request.getProject(), request).getSecond();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @GetMapping(value = "/rule", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getRule(@RequestParam("project") String project, @RequestParam("model") String modelId) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val rule = indexPlanService.getRule(project, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, rule, "");
    }

    @PutMapping(value = "/agg_index_count", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse calculateAggIndexCombination(@RequestBody UpdateRuleBasedCuboidRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());

        val aggIndexCount = indexPlanService.calculateAggIndexCount(request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, aggIndexCount, "");
    }

    @PostMapping(value = "/table_index", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse createTableIndex(@Valid @RequestBody CreateTableIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        val response = indexPlanService.createTableIndex(request.getProject(), request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PutMapping(value = "/table_index", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse updateTableIndex(@Valid @RequestBody CreateTableIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        checkRequiredArg("id", request.getId());
        val response = indexPlanService.updateTableIndex(request.getProject(), request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @DeleteMapping(value = "/table_index/{project}/{model}/{id}", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse deleteTableIndex(@PathVariable("project") String project,
            @PathVariable("model") String modelId, @PathVariable("id") Long id) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        checkRequiredArg("id", id);
        indexPlanService.removeTableIndex(project, modelId, id);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @GetMapping(value = "/table_index", produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getTableIndex(@RequestParam(value = "project") String project,
            @RequestParam(value = "model") String modelId,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val tableIndexs = indexPlanService.getTableIndexs(project, modelId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                getDataResponse("table_indexs", tableIndexs, offset, limit), "");
    }

}
