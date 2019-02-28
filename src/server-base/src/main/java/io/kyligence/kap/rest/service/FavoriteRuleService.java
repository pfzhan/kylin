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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.query.AccelerateRatio;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.rest.response.ImportSqlResponse;
import io.kyligence.kap.rest.response.SQLValidateResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;
import io.kyligence.kap.smart.query.validator.AbstractSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.kyligence.kap.smart.query.validator.SqlSyntaxValidator;

@Component("favoriteRuleService")
public class FavoriteRuleService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleService.class);

    public Map<String, Object> getFrequencyRule(String project) {
        FavoriteRule frequencyRule = getFavoriteRule(project, FavoriteRule.FREQUENCY_RULE_NAME);

        Map<String, Object> result = Maps.newHashMap();
        result.put(FavoriteRule.ENABLE, frequencyRule.isEnabled());
        FavoriteRule.AbstractCondition condition = frequencyRule.getConds().get(0);
        result.put("freqValue", Float.valueOf(((FavoriteRule.Condition) condition).getRightThreshold()));

        return result;
    }

    public Map<String, Object> getSubmitterRule(String project) {
        FavoriteRule submitterRule = getFavoriteRule(project, FavoriteRule.SUBMITTER_RULE_NAME);

        Map<String, Object> result = Maps.newHashMap();
        result.put(FavoriteRule.ENABLE, submitterRule.isEnabled());
        List<String> users = Lists.newArrayList();
        for (FavoriteRule.AbstractCondition cond : submitterRule.getConds()) {
            users.add(((FavoriteRule.Condition) cond).getRightThreshold());
        }
        result.put("users", users);
        result.put("groups", Lists.newArrayList());

        return result;
    }

    public Map<String, Object> getDurationRule(String project) {
        FavoriteRule durationRule = getFavoriteRule(project, FavoriteRule.DURATION_RULE_NAME);

        Map<String, Object> result = Maps.newHashMap();
        result.put(FavoriteRule.ENABLE, durationRule.isEnabled());
        FavoriteRule.AbstractCondition condition = durationRule.getConds().get(0);
        result.put("durationValue",
                Lists.newArrayList(Long.valueOf(((FavoriteRule.Condition) condition).getLeftThreshold()),
                        Long.valueOf(((FavoriteRule.Condition) condition).getRightThreshold())));

        return result;
    }

    FavoriteRule getFavoriteRule(String project, String ruleName) {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        Preconditions.checkArgument(ruleName != null && StringUtils.isNotEmpty(ruleName));

        FavoriteRule favoriteRule = getFavoriteRuleManager(project).getByName(ruleName);

        if (favoriteRule == null)
            throw new NotFoundException(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), ruleName));

        return favoriteRule;
    }

    @Transaction(project = 0)
    public void updateRegularRule(String project, FavoriteRuleUpdateRequest request, String ruleName) {
        FavoriteRule rule = getFavoriteRule(project, ruleName);
        rule.setEnabled(request.isEnable());

        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            FavoriteRule.Condition freqCond = (FavoriteRule.Condition) rule.getConds().get(0);
            freqCond.setRightThreshold(request.getFreqValue());
            conds.add(freqCond);
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            for (String user : request.getUsers()) {
                conds.add(new FavoriteRule.Condition(null, user));
            }
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            if (request.getDurationValue().length < 2)
                throw new IllegalArgumentException("Duration rule should have both left threshold and right threshold");
            FavoriteRule.Condition durationCond = (FavoriteRule.Condition) rule.getConds().get(0);
            durationCond.setLeftThreshold(request.getDurationValue()[0]);
            durationCond.setRightThreshold(request.getDurationValue()[1]);
            conds.add(durationCond);
            break;
        default:
            break;
        }

        rule.setConds(conds);
        getFavoriteRuleManager(project).updateRule(conds, rule.isEnabled(), ruleName);
        NFavoriteScheduler favoriteScheduler = getFavoriteScheduler(project);
        if (!favoriteScheduler.hasStarted()) {
            throw new RuntimeException("Auto favorite scheduler for " + project + " has not been started");
        }
        favoriteScheduler.scheduleAutoFavorite();
    }

    @Transaction(project = 0)
    public void deleteFavoriteQuery(String project, String uuid) {
        FavoriteQuery favoriteQuery = getFavoriteQueryManager(project).getByUuid(uuid);
        if (favoriteQuery == null)
            throw new BadRequestException(String.format(MsgPicker.getMsg().getFAVORITE_QUERY_NOT_EXIST(), uuid));

        String channel = favoriteQuery.getChannel();
        String sqlPattern = favoriteQuery.getSqlPattern();
        // put to blacklist
        if (channel.equals(FavoriteQuery.CHANNEL_FROM_RULE)) {
            FavoriteRule.SQLCondition sqlCondition = new FavoriteRule.SQLCondition(sqlPattern);
            getFavoriteRuleManager(project).appendSqlPatternToBlacklist(sqlCondition);
        }
        // delete favorite query
        getFavoriteQueryManager(project).delete(favoriteQuery);
    }

    public List<FavoriteRule.SQLCondition> getBlacklistSqls(String project) {
        FavoriteRule blacklist = getFavoriteRuleManager(project).getByName(FavoriteRule.BLACKLIST_NAME);
        return blacklist.getConds().stream().map(cond -> (FavoriteRule.SQLCondition) cond)
                .sorted(Comparator.comparingLong(FavoriteRule.SQLCondition::getCreateTime).reversed())
                .collect(Collectors.toList());
    }

    @Transaction(project = 1)
    public void removeBlacklistSql(String id, String project) {
        getFavoriteRuleManager(project).removeSqlPatternFromBlacklist(id);
    }

    private Map<String, SQLValidateResult> batchSqlValidate(List<String> sqls, String project) {
        Map<String, SQLValidateResult> map;
        try {
            AbstractSQLValidator sqlValidator = new SqlSyntaxValidator(getConfig(), project, new MockupQueryExecutor());
            map = sqlValidator.batchValidate(sqls.toArray(new String[0]));
        } catch (IOException e) {
            throw new InternalErrorException(MsgPicker.getMsg().getFAIL_TO_VERIFY_SQL(), e);
        }

        return map;
    }

    public Map<String, Object> importSqls(MultipartFile[] files, String project) {
        Map<String, Object> result = Maps.newHashMap();
        List<String> sqls = Lists.newArrayList();
        List<String> filesParseFailed = Lists.newArrayList();

        // parse file to sqls
        for (MultipartFile file : files) {
            try {
                sqls.addAll(transformFileToSqls(file));
            } catch (Exception ex) {
                logger.error("Error caught when parsing file {} because {} ", file.getOriginalFilename(),
                        ex.getMessage());
                filesParseFailed.add(file.getOriginalFilename());
            }
        }

        List<ImportSqlResponse> sqlData = Lists.newArrayList();
        int capableSqlNum = 0;

        //sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(sqls, project);
        int id = 0;

        for (Map.Entry<String, SQLValidateResult> entry : map.entrySet()) {
            String sql = entry.getKey();
            SQLValidateResult validateResult = entry.getValue();

            if (validateResult.isCapable())
                capableSqlNum++;

            ImportSqlResponse sqlResponse = new ImportSqlResponse(sql, validateResult.isCapable());
            sqlResponse.setId(id);
            sqlResponse.setSqlAdvices(validateResult.getSqlAdvices());
            sqlData.add(sqlResponse);

            id++;
        }

        // make sql grammar failed sqls ordered first
        sqlData.sort((object1, object2) -> {
            boolean capable1 = object1.isCapable();
            boolean capable2 = object2.isCapable();

            if (capable1 && !capable2)
                return 1;

            if (capable2 && !capable1)
                return -1;

            return 0;
        });

        result.put("data", sqlData);
        result.put("size", sqlData.size());
        result.put("capable_sql_num", capableSqlNum);

        if (!filesParseFailed.isEmpty()) {
            result.put("msg", Joiner.on(",").join(filesParseFailed) + " parse failed");
        }

        return result;
    }

    List<String> transformFileToSqls(MultipartFile file) throws IOException {
        List<String> sqls = new ArrayList<>();

        String content = new String(file.getBytes(), "UTF-8");

        if (content.isEmpty()) {
            return sqls;
        }
        content = QueryUtil.removeCommentInSql(content);
        String[] sqlsArray = content.split(";");
        if (sqlsArray.length == 0) {
            return sqls;
        }
        for (String sql : sqlsArray) {
            if (sql == null || sql.length() == 0 || sql.replace('\n', ' ').trim().length() == 0) {
                continue;
            }
            sqls.add(sql.replace('\n', ' ').trim());
        }

        return sqls;
    }

    public SQLValidateResponse sqlValidate(String project, String sql) {
        // sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(Lists.newArrayList(sql), project);
        SQLValidateResult result = map.get(sql);

        return new SQLValidateResponse(result.isCapable(), result.getSqlAdvices());
    }

    public double getAccelerateRatio(String project) {
        AccelerateRatioManager ratioManager = getAccelerateRatioManager(project);
        AccelerateRatio accelerateRatio = ratioManager.get();
        if (accelerateRatio == null || accelerateRatio.getOverallQueryNum() == 0)
            return 0;

        return accelerateRatio.getQueryNumOfMarkedAsFavorite() / (double) accelerateRatio.getOverallQueryNum();
    }
}
