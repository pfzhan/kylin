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

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.request.SQLValidateRequest;
import io.kyligence.kap.rest.response.SQLValidateResponse;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api/query/favorite_queries", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class FavoriteQueryController extends NBasicController {

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<Map<String, Integer>> createFavoriteQuery(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                favoriteQueryService.createFavoriteQuery(request.getProject(), request), "");
    }

    @ApiOperation(value = "listFavoriteQuery (update)", notes = "Update Param: sort_by; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<FavoriteQuery>>> listFavoriteQuery(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "sort_by", required = false, defaultValue = "") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<FavoriteQuery> filteredAndSortedFQ = favoriteQueryService.filterAndSortFavoriteQueries(project, sortBy,
                reverse, status);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(filteredAndSortedFQ, offset, limit),
                "");
    }

    @GetMapping(value = "/size")
    @ResponseBody
    public EnvelopeResponse<Map<String, Integer>> getFQSizeInDifferentStatus(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                favoriteQueryService.getFQSizeInDifferentStatus(project), "");
    }

    @DeleteMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> batchDeleteFQs(@RequestParam(value = "project") String project,
            @RequestParam(value = "uuids") List<String> uuids,
            @RequestParam(value = "block", required = false, defaultValue = "false") boolean block) {
        checkProjectName(project);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(uuids), "Ids should not be empty");
        uuids.forEach(uuid -> checkId(uuid));
        favoriteRuleService.batchDeleteFQs(project, uuids, block);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/threshold")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getAccelerateTips(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, favoriteQueryService.getAccelerateTips(project), "");
    }

    @PutMapping(value = "/accelerate")
    @ResponseBody
    public EnvelopeResponse<Map<String, List<String>>> acceptAccelerate(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        Preconditions.checkNotNull(request.getSqls());
        Map<String, List<String>> result = favoriteQueryService.acceptAccelerate(request.getProject(),
                request.getSqls());

        NMetricsGroup.counterInc(NMetricsName.FQ_FE_INVOKED, NMetricsCategory.PROJECT, request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "acceptAccelerate (update)", notes = "Update Param: accelerate_size")
    @PutMapping(value = "/accept")
    @ResponseBody
    public EnvelopeResponse<String> acceptAccelerate(@RequestParam(value = "project") String project,
            @RequestParam(value = "accelerate_size") int accelerateSize) {
        checkProjectName(project);
        favoriteQueryService.acceptAccelerate(project, accelerateSize);

        NMetricsGroup.counterInc(NMetricsName.FQ_FE_INVOKED, NMetricsCategory.PROJECT, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "ignoreAccelerate (update)", notes = "Update Param: ignore_size")
    @PutMapping(value = "/ignore")
    @ResponseBody
    public EnvelopeResponse<String> ignoreAccelerate(@RequestParam(value = "project") String project,
            @RequestParam(value = "ignore_size") int ignoreSize) {
        checkProjectName(project);
        favoriteQueryService.ignoreAccelerate(project, ignoreSize);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getFavoriteRules (update)", notes = "Update Param: freq_enable, freq_value, count_enable, count_value, duration_enable, min_duration, max_duration, submitter_enable, user_groups")
    @GetMapping(value = "/rules")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getFavoriteRules(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, favoriteRuleService.getFavoriteRules(project), "");
    }

    @ApiOperation(value = "updateFavoriteRules (update)", notes = "Update Param: freq_enable, freq_value, count_enable, count_value, duration_enable, min_duration, max_duration, submitter_enable, user_groups")
    @PutMapping(value = "/rules")
    @ResponseBody
    public EnvelopeResponse<String> updateFavoriteRules(@RequestBody FavoriteRuleUpdateRequest request) {
        checkProjectName(request.getProject());
        checkUpdateFavoriteRuleArgs(request);
        favoriteRuleService.updateRegularRule(request.getProject(), request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    private void checkUpdateFavoriteRuleArgs(FavoriteRuleUpdateRequest request) {
        // either disabled or arguments not empty
        if (request.isFreqEnable() && StringUtils.isEmpty(request.getFreqValue()))
            throw new BadRequestException("Frequency rule value is empty");

        if (request.isDurationEnable()
                && (StringUtils.isEmpty(request.getMinDuration()) || StringUtils.isEmpty(request.getMaxDuration())))
            throw new BadRequestException("Duration rule values are empty");
    }

    @ApiOperation(value = "getBlacklist (update)", notes = "Update Response: total_size")
    @GetMapping(value = "/blacklist")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<FavoriteRule.SQLCondition>>> getBlacklist(
            @RequestParam("project") String project, // 
            @RequestParam(value = "sql") String sql, //
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        checkProjectName(project);
        List<FavoriteRule.SQLCondition> blacklistSqls = favoriteRuleService.getBlacklistSqls(project, sql);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(blacklistSqls, offset, limit), "");
    }

    @ApiOperation(value = "removeBlacklistSql (update)", notes = "Add URL: {id}; Update Param: id")
    @DeleteMapping(value = "/blacklist/{id:.+}")
    @ResponseBody
    public EnvelopeResponse<String> removeBlacklistSql(@PathVariable("id") String id,
            @RequestParam("project") String project) {
        checkProjectName(project);
        favoriteRuleService.removeBlacklistSql(id, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "importSqls (response)", notes = "sql_advices")
    @PostMapping(value = "/sql_files")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> importSqls(@RequestParam("project") String project,
            @RequestParam("files") MultipartFile[] files) {
        Map<String, Object> data = favoriteRuleService.importSqls(files, project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, (String) data.get("msg"));
    }

    @ApiOperation(value = "sqlValidate (update)", notes = "Update Response: incapable_reason, sql_advices")
    @PutMapping(value = "/sql_validation")
    @ResponseBody
    public EnvelopeResponse<SQLValidateResponse> sqlValidate(@RequestBody SQLValidateRequest request) {
        checkProjectName(request.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                favoriteRuleService.sqlValidate(request.getProject(), request.getSql()), "");
    }

    @GetMapping(value = "/accelerate_ratio")
    @ResponseBody
    public EnvelopeResponse<Double> getAccelerateRatio(@RequestParam("project") String project) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, favoriteRuleService.getAccelerateRatio(project), "");
    }
}
