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
import static io.kyligence.kap.metadata.favorite.FavoriteRule.EFFECTIVE_DAYS_MAX;
import static io.kyligence.kap.metadata.favorite.FavoriteRule.EFFECTIVE_DAYS_MIN;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_COUNT_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_DURATION_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_EFFECTIVE_DAYS;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_FREQUENCY_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_MIN_HIT_COUNT;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_REC_RULE_VALUE;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_UPDATE_FREQUENCY;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_RANGE;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.favorite.AbstractAsyncTask;
import io.kyligence.kap.metadata.favorite.AsyncAccelerationTask;
import io.kyligence.kap.metadata.favorite.AsyncTaskManager;
import io.kyligence.kap.rest.response.ProjectStatisticsResponse;
import io.kyligence.kap.rest.service.ProjectSmartService;
import io.kyligence.kap.rest.service.QueryHistoryService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/projects", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NProjectSmartController extends NBasicController {

    @Autowired
    @Qualifier("projectSmartService")
    private ProjectSmartService projectSmartService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("queryHistoryService")
    private QueryHistoryService qhService;

    @ApiOperation(value = "getFavoriteRules", tags = {
            "SM" }, notes = "Update Param: freq_enable, freq_value, count_enable, count_value, duration_enable, min_duration, max_duration, submitter_enable, user_groups")
    @GetMapping(value = "/{project:.+}/favorite_rules")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getFavoriteRules(@PathVariable(value = "project") String project) {
        checkProjectName(project);
        aclEvaluate.checkProjectWritePermission(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectSmartService.getFavoriteRules(project), "");
    }

    @ApiOperation(value = "statistics", tags = { "SM" })
    @GetMapping(value = "/statistics")
    @ResponseBody
    public EnvelopeResponse<ProjectStatisticsResponse> getDashboardStatistics(@RequestParam("project") String project) {
        checkProjectName(project);
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics(project);
        projectStatistics.setLastWeekQueryCount(qhService.getLastWeekQueryCount(project));
        projectStatistics.setUnhandledQueryCount(qhService.getQueryCountToAccelerate(project));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projectStatistics, "");
    }

    @ApiOperation(value = "getAcceleration", tags = { "AI" })
    @GetMapping(value = "/acceleration")
    @ResponseBody
    public EnvelopeResponse<Boolean> isAccelerating(@RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        AbstractAsyncTask asyncTask = AsyncTaskManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                ((AsyncAccelerationTask) asyncTask).isAlreadyRunning(), "");
    }

    @ApiOperation(value = "updateAcceleration", tags = { "AI" })
    @PutMapping(value = "/acceleration")
    @ResponseBody
    public EnvelopeResponse<Object> accelerate(@RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        Set<Integer> deltaRecs = projectSmartService.accelerateManually(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, deltaRecs.size(), "");
    }

    @ApiOperation(value = "statistics", tags = { "AI" })
    @PostMapping(value = "/acceleration_tag")
    @ResponseBody
    public EnvelopeResponse<Object> cleanAsyncAccelerateTag(@RequestParam("project") String project,
            @RequestParam("user") String user) {
        checkProjectName(project);
        checkRequiredArg("user", user);
        AsyncTaskManager.cleanAccelerationTagByUser(project, user);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "updateFavoriteRules", tags = {
            "SM" }, notes = "Update Param: freq_enable, freq_value, count_enable, count_value, duration_enable, min_duration, max_duration, submitter_enable, user_groups")
    @PutMapping(value = "/{project:.+}/favorite_rules")
    @ResponseBody
    public EnvelopeResponse<String> updateFavoriteRules(@RequestBody FavoriteRuleUpdateRequest request) {
        checkProjectName(request.getProject());
        checkProjectUnmodifiable(request.getProject());
        checkUpdateFavoriteRuleArgs(request);
        projectSmartService.updateRegularRule(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    protected static void checkUpdateFavoriteRuleArgs(FavoriteRuleUpdateRequest request) {
        // either disabled or arguments not empty
        if (request.isFreqEnable() && StringUtils.isEmpty(request.getFreqValue())) {
            throw new KylinException(EMPTY_FREQUENCY_RULE_VALUE,
                    MsgPicker.getMsg().getFrequencyThresholdCanNotEmpty());
        }

        if (request.isDurationEnable()
                && (StringUtils.isEmpty(request.getMinDuration()) || StringUtils.isEmpty(request.getMaxDuration()))) {
            throw new KylinException(EMPTY_DURATION_RULE_VALUE, MsgPicker.getMsg().getDelayThresholdCanNotEmpty());
        }

        if (request.isCountEnable() && StringUtils.isEmpty(request.getCountValue())) {
            throw new KylinException(EMPTY_COUNT_RULE_VALUE, MsgPicker.getMsg().getFrequencyThresholdCanNotEmpty());
        }

        if (request.isRecommendationEnable() && StringUtils.isEmpty(request.getRecommendationsValue().trim())) {
            throw new KylinException(EMPTY_REC_RULE_VALUE, MsgPicker.getMsg().getRecommendationLimitNotEmpty());
        }

        if (StringUtils.isEmpty(request.getMinHitCount())) {
            throw new KylinException(EMPTY_MIN_HIT_COUNT, MsgPicker.getMsg().getMinHitCountNotEmpty());
        }

        if (StringUtils.isEmpty(request.getEffectiveDays())) {
            throw new KylinException(EMPTY_EFFECTIVE_DAYS, MsgPicker.getMsg().getEffectiveDaysNotEmpty());
        }

        if (StringUtils.isEmpty(request.getUpdateFrequency())) {
            throw new KylinException(EMPTY_UPDATE_FREQUENCY, MsgPicker.getMsg().getUpdateFrequencyNotEmpty());
        }
        checkRange(request.getRecommendationsValue(), 0, Integer.MAX_VALUE);
        checkRange(request.getMinHitCount(), 1, Integer.MAX_VALUE);
        checkRange(request.getEffectiveDays(), EFFECTIVE_DAYS_MIN, EFFECTIVE_DAYS_MAX);
        checkRange(request.getUpdateFrequency(), 1, Integer.MAX_VALUE);
    }

    private static void checkRange(String value, int start, int end) {
        boolean inRightRange;
        try {
            int i = Integer.parseInt(value);
            inRightRange = (i >= start && i <= end);
        } catch (Exception e) {
            inRightRange = false;
        }

        if (!inRightRange) {
            throw new KylinException(INVALID_RANGE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidRange(), value, start, end));
        }
    }

}
