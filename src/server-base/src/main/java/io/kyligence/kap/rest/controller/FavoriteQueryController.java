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
import io.kyligence.kap.rest.service.FavoriteQueryService;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.QueryFilterRequest;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping(value = "/query/favorite_queries", produces = { "application/vnd.apache.kylin-v2+json" })
public class FavoriteQueryController extends NBasicController {

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse manualFavorite(@RequestBody FavoriteRequest request) throws PersistentException {
        favoriteQueryService.manualFavorite(request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse listFavoriteQuery(@RequestParam(value = "project") String project,
                                  @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
                                  @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        List<FavoriteQuery> favoriteQueries = favoriteQueryService.getFavoriteQueriesByPage(project, limit, offset);
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("favorite_queries", favoriteQueries);
        data.put("size", favoriteQueries.size());
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
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getFrequencyRule(project), "");
    }

    @RequestMapping(value = "/rules/submitter", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getSubmitterRule(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getSubmitterRule(project), "");
    }

    @RequestMapping(value = "/rules/duration", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getDurationRule(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getDurationRule(project), "");
    }

    @RequestMapping(value = "/rules/frequency", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateFrequencyRule(@RequestBody QueryFilterRequest request) throws IOException {
        favoriteQueryService.updateQueryFilterRule(request, QueryFilterRule.FREQUENCY_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/submitter", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateSubmitterRule(@RequestBody QueryFilterRequest request) throws IOException {
        favoriteQueryService.updateQueryFilterRule(request, QueryFilterRule.SUBMITTER_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules/duration", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateDurationRule(@RequestBody QueryFilterRequest request) throws IOException {
        favoriteQueryService.updateQueryFilterRule(request, QueryFilterRule.DURATION_RULE_NAME);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }
}
