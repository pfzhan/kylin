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
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.PagingUtil;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.QueryHistoryService;
import org.apache.kylin.rest.request.QueryFilterRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping(value = "/query/favorite_queries", produces = { "application/vnd.apache.kylin-v2+json" })
public class FavoriteQueryController extends NBasicController {

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    @Autowired
    @Qualifier("queryHistoryService")
    private QueryHistoryService queryHistoryService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse favorite(@RequestParam(value = "project") String project,
                                     @RequestBody List<String> queries) throws IOException {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.favorite(project, queries), "");
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE)
    public EnvelopeResponse unFavorite(@RequestParam(value = "project") String project,
                                       @RequestBody List<String> favoriteQueries) throws Exception {
        favoriteQueryService.unFavorite(project, favoriteQueries);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse listFavoriteQuery(@RequestParam(value = "project") String project,
                                  @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
                                  @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        List<FavoriteQuery> favoriteQueries = favoriteQueryService.getAllFavoriteQueries(project);
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("favorite_queries", PagingUtil.cutPage(favoriteQueries, pageOffset, pageSize));
        data.put("size", favoriteQueries.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/candidates", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse getCandidates(@RequestBody QueryFilterRequest request,
                                           @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
                                           @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        List<QueryHistory> queryHistories = queryHistoryService.getQueryHistoriesByRules(request.getRules(),
                favoriteQueryService.getCandidates(request.getProject()));
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("candidates", PagingUtil.cutPage(queryHistories, pageOffset, pageSize));
        data.put("size", queryHistories.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/threshold", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse isTimeToAccelerate(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.isTimeToAccelerate(project), "");
    }

    @RequestMapping(value = "/accept", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse acceptAccelerate(@RequestParam(value = "project") String project) throws Exception {
        favoriteQueryService.acceptAccelerate(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getFilterRule(@RequestParam(value = "project") String project) {

        List<QueryFilterRule> rules = favoriteQueryService.getQueryFilterRules(project);
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("rules", rules);
        data.put("size", rules.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/rules", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse saveFilterRule(@RequestBody QueryFilterRequest request) throws IOException {
        favoriteQueryService.saveQueryFilterRule(request.getProject(), request.getRules());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules", method = RequestMethod.DELETE)
    @ResponseBody
    public EnvelopeResponse deleteFilterRule(@RequestParam(value = "project") String project,
                                             @RequestBody List<String> ruleUuids) throws IOException {
        favoriteQueryService.deleteQueryFilterRule(project, ruleUuids);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/{project}", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse applyAll(@PathVariable("project") String project) throws IOException {
        favoriteQueryService.applyAll(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/automatic/{project}", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse setAutoMarkFavorite(@PathVariable("project") String project) throws IOException {
        favoriteQueryService.markAutomatic(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }
}
