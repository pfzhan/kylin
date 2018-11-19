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

import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.PagingUtil;
import io.kyligence.kap.rest.request.AppendBlacklistSqlRequest;
import io.kyligence.kap.rest.request.WhitelistUpdateRequest;
import io.kyligence.kap.rest.response.FavoriteRuleResponse;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/query/favorite_queries", produces = { "application/vnd.apache.kylin-v2+json" })
public class FavoriteQueryController extends NBasicController {

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse manualFavorite(@RequestBody FavoriteRequest request) throws IOException {
        favoriteQueryService.manualFavorite(request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse listFavoriteQuery(@RequestParam(value = "project") String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("favorite_queries", favoriteQueryService.getFavoriteQueriesByPage(project, limit, offset));
        data.put("size", favoriteQueryService.getFavoriteQuerySize(project));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/threshold", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getAccelerateTips(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getAccelerateTips(project), "");
    }

    @RequestMapping(value = "/accept", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse acceptAccelerate(@RequestParam(value = "project") String project,
            @RequestParam(value = "accelerateSize") int accelerateSize) throws Exception {
        favoriteQueryService.acceptAccelerate(project, accelerateSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/ignore/{project}", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse ignoreAccelerate(@PathVariable(value = "project") String project) {
        favoriteQueryService.ignoreAccelerate(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/frequency", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getFrequencyRule(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getFrequencyRule(project), "");
    }

    @RequestMapping(value = "/rules/submitter", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getSubmitterRule(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getSubmitterRule(project), "");
    }

    @RequestMapping(value = "/rules/duration", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getDurationRule(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getDurationRule(project), "");
    }

    @RequestMapping(value = "/rules/frequency", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateFrequencyRule(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        favoriteRuleService.updateRegularRule(request, FavoriteRule.FREQUENCY_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/submitter", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateSubmitterRule(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        favoriteRuleService.updateRegularRule(request, FavoriteRule.SUBMITTER_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/duration", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateDurationRule(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        favoriteRuleService.updateRegularRule(request, FavoriteRule.DURATION_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/blacklist", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse appendSqlToBlacklist(@RequestBody AppendBlacklistSqlRequest request) throws IOException {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                favoriteRuleService.appendSqlToBlacklist(request.getSql(), request.getProject()), "");
    }

    @RequestMapping(value = "/blacklist", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getBlacklist(@RequestParam("project") String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        Map<String, Object> data = Maps.newHashMap();
        List<FavoriteRuleResponse> blacklistSqls = favoriteRuleService.getBlacklistSqls(project);
        data.put("sqls", PagingUtil.cutPage(blacklistSqls, offset, limit));
        data.put("size", blacklistSqls.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/blacklist", method = RequestMethod.DELETE)
    @ResponseBody
    public EnvelopeResponse removeBlacklistSql(@RequestParam("id") String id, @RequestParam("project") String project)
            throws IOException {
        favoriteRuleService.removeBlacklistSql(id, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/whitelist", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse loadSqlsToWhitelist(@RequestParam("file") MultipartFile file,
            @RequestParam("project") String project) throws IOException {
        favoriteRuleService.loadSqlsToWhitelist(file, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/whitelist", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateWhitelist(@RequestBody WhitelistUpdateRequest request) throws IOException {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                favoriteRuleService.updateWhitelistSql(request.getSql(), request.getId(), request.getProject()), "");
    }

    @RequestMapping(value = "/whitelist", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getWhitelist(@RequestParam("project") String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        Map<String, Object> data = Maps.newHashMap();
        List<FavoriteRuleResponse> whitelistSqls = favoriteRuleService.getWhitelist(project);
        data.put("sqls", PagingUtil.cutPage(whitelistSqls, offset, limit));
        data.put("size", whitelistSqls.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/whitelist", method = RequestMethod.DELETE)
    @ResponseBody
    public EnvelopeResponse removeWhitelistSql(@RequestParam("id") String id, @RequestParam("project") String project)
            throws IOException {
        favoriteRuleService.removeWhitelistSql(id, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "rules/impact", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getRulesOverallImpact(@RequestParam("project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                favoriteRuleService.getFavoriteRuleOverallImpact(project), "");
    }
}
