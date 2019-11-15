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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.request.SQLValidateRequest;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.FavoriteRuleService;

@RestController
@RequestMapping(value = "/api/query/favorite_queries", produces = { "application/vnd.apache.kylin-v2+json" })
public class FavoriteQueryController extends NBasicController {

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    public EnvelopeResponse createFavoriteQuery(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                favoriteQueryService.createFavoriteQuery(request.getProject(), request), "");
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
        List<FavoriteQuery> filteredAndSortedFQ = favoriteQueryService.filterAndSortFavoriteQueries(project, sortBy,
                reverse, status);
        data.put("favorite_queries", PagingUtil.cutPage(filteredAndSortedFQ, offset, limit));
        data.put("size", filteredAndSortedFQ.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/size", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getFQSizeInDifferentStatus(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getFQSizeInDifferentStatus(project),
                "");
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE)
    @ResponseBody
    public EnvelopeResponse batchDeleteFQs(@RequestParam(value = "project") String project,
            @RequestParam(value = "uuids") List<String> uuids,
            @RequestParam(value = "block", required = false, defaultValue = "false") boolean block) {
        checkProjectName(project);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(uuids), "Ids should not be empty");
        uuids.forEach(uuid -> checkId(uuid));
        favoriteRuleService.batchDeleteFQs(project, uuids, block);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/threshold", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getAccelerateTips(@RequestParam(value = "project") String project) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteQueryService.getAccelerateTips(project), "");
    }

    @RequestMapping(value = "/accelerate", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse acceptAccelerate(@RequestBody FavoriteRequest request) {
        checkProjectName(request.getProject());
        Preconditions.checkNotNull(request.getSqls());
        Map<String, List<String>> result = favoriteQueryService.acceptAccelerate(request.getProject(),
                request.getSqls());

        NMetricsGroup.counterInc(NMetricsName.FQ_FE_INVOKED, NMetricsCategory.PROJECT, request.getProject());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/accept", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse acceptAccelerate(@RequestParam(value = "project") String project,
            @RequestParam(value = "accelerateSize") int accelerateSize) {
        checkProjectName(project);
        favoriteQueryService.acceptAccelerate(project, accelerateSize);

        NMetricsGroup.counterInc(NMetricsName.FQ_FE_INVOKED, NMetricsCategory.PROJECT, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/ignore", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse ignoreAccelerate(@RequestParam(value = "project") String project,
            @RequestParam(value = "ignoreSize") int ignoreSize) {
        checkProjectName(project);
        favoriteQueryService.ignoreAccelerate(project, ignoreSize);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/rules", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getFavoriteRules(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getFavoriteRules(project), "");
    }

    @RequestMapping(value = "/rules", method = RequestMethod.PUT)
    @ResponseBody
    public EnvelopeResponse updateFavoriteRules(@RequestBody FavoriteRuleUpdateRequest request) throws IOException {
        checkProjectName(request.getProject());
        checkUpdateFavoriteRuleArgs(request);
        favoriteRuleService.updateRegularRule(request.getProject(), request);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
    }

    private void checkUpdateFavoriteRuleArgs(FavoriteRuleUpdateRequest request) {
        // either disabled or arguments not empty
        if (request.isFreqEnable() && StringUtils.isEmpty(request.getFreqValue()))
            throw new BadRequestException("Frequency rule value is empty");

        if (request.isDurationEnable()
                && (StringUtils.isEmpty(request.getMinDuration()) || StringUtils.isEmpty(request.getMaxDuration())))
            throw new BadRequestException("Duration rule values are empty");
    }

    @RequestMapping(value = "/blacklist", method = RequestMethod.GET)
    @ResponseBody
    public EnvelopeResponse getBlacklist(@RequestParam("project") String project,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        checkProjectName(project);
        Map<String, Object> data = Maps.newHashMap();
        List<FavoriteRule.SQLCondition> blacklistSqls = favoriteRuleService.getBlacklistSqls(project, sql);
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
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, favoriteRuleService.getAccelerateRatio(project), "");
    }
}
