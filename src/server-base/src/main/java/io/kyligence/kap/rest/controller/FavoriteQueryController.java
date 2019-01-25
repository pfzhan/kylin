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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.rest.request.SQLValidateRequest;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.FavoriteRuleService;

@RestController
@RequestMapping(value = "/query/favorite_queries", produces = { "application/vnd.apache.kylin-v2+json" })
public class FavoriteQueryController extends NBasicController {

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse createFavoriteQuery(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        favoriteQueryService.createFavoriteQuery(request.getProject(), request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse listFavoriteQuery(@RequestParam(value = "project") String project,
            @RequestParam(value = "sortBy", required = false, defaultValue = "") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean reverse,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        HashMap<String, Object> data = Maps.newHashMap();
        List<FavoriteQuery> filteredAndSortedFQ = favoriteQueryService.filterAndSortFavoriteQueries(project, sortBy, reverse, status);
        data.put("favorite_queries", PagingUtil.cutPage(filteredAndSortedFQ, offset, limit));
        data.put("size", filteredAndSortedFQ.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE)
    @ResponseBody
    public EnvelopeResponse deleteFavoriteQuery(@RequestParam(value = "project") String project,
                                                @RequestParam(value = "uuid") String uuid) {
        checkProjectName(project);
        checkId(uuid);
        favoriteRuleService.deleteFavoriteQuery(project, uuid);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/threshold", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getAccelerateTips(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getAccelerateTips(project), "");
    }

    @RequestMapping(value = "/accept", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse acceptAccelerate(@RequestParam(value = "project") String project,
            @RequestParam(value = "accelerateSize") int accelerateSize) {
        checkProjectName(project);
        favoriteQueryService.acceptAccelerate(project, accelerateSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/ignore/{project}", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse ignoreAccelerate(@PathVariable(value = "project") String project) {
        checkProjectName(project);
        favoriteQueryService.ignoreAccelerate(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/frequency", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getFrequencyRule(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getFrequencyRule(project), "");
    }

    @RequestMapping(value = "/rules/submitter", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getSubmitterRule(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getSubmitterRule(project), "");
    }

    @RequestMapping(value = "/rules/duration", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getDurationRule(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getDurationRule(project), "");
    }

    @RequestMapping(value = "/rules/frequency", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateFrequencyRule(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        checkProjectName(request.getProject());
        favoriteRuleService.updateRegularRule(request.getProject(), request, FavoriteRule.FREQUENCY_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/submitter", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateSubmitterRule(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        checkProjectName(request.getProject());
        favoriteRuleService.updateRegularRule(request.getProject(), request, FavoriteRule.SUBMITTER_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/duration", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateDurationRule(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        checkProjectName(request.getProject());
        favoriteRuleService.updateRegularRule(request.getProject(), request, FavoriteRule.DURATION_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/blacklist", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getBlacklist(@RequestParam("project") String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        checkProjectName(project);
        Map<String, Object> data = Maps.newHashMap();
        List<FavoriteRule.SQLCondition> blacklistSqls = favoriteRuleService.getBlacklistSqls(project);
        data.put("sqls", PagingUtil.cutPage(blacklistSqls, offset, limit));
        data.put("size", blacklistSqls.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/blacklist", method = RequestMethod.DELETE)
    @ResponseBody
    public EnvelopeResponse removeBlacklistSql(@RequestParam("id") String id, @RequestParam("project") String project) {
        checkProjectName(project);
        favoriteRuleService.removeBlacklistSql(id, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/sql_files", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse importSqls(@RequestParam("files") MultipartFile[] files,
                                       @RequestParam("project") String project) {
        checkProjectName(project);
        Map<String, Object> data = favoriteRuleService.importSqls(files, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, (String) data.get("msg"));
    }

    @RequestMapping(value = "/sql_validation", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse sqlValidate(@RequestBody SQLValidateRequest request) {
        checkProjectName(request.getProject());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                favoriteRuleService.sqlValidate(request.getProject(), request.getSql()), "");
    }

    @RequestMapping(value = "/accelerate_ratio", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getAccelerateRatio(@RequestParam("project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                favoriteRuleService.getAccelerateRatio(project), "");
    }
}
