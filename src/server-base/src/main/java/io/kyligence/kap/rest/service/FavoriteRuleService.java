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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
import lombok.val;

@Component("favoriteRuleService")
public class FavoriteRuleService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleService.class);
    private static final List<String> favoriteRuleNames = Lists.newArrayList(FavoriteRule.COUNT_RULE_NAME,
            FavoriteRule.FREQUENCY_RULE_NAME, FavoriteRule.DURATION_RULE_NAME, FavoriteRule.SUBMITTER_RULE_NAME,
            FavoriteRule.SUBMITTER_GROUP_RULE_NAME);

    private static final String DEFAULT_SCHEMA = "DEFAULT";

    @Autowired
    private AclEvaluate aclEvaluate;

    public Map<String, Object> getFavoriteRules(String project) {
        Map<String, Object> result = Maps.newHashMap();

        for (String ruleName : favoriteRuleNames) {
            getSingleRule(project, ruleName, result);
        }

        return result;
    }

    private void getSingleRule(String project, String ruleName, Map<String, Object> result) {
        FavoriteRule rule = getFavoriteRule(project, ruleName);
        List<FavoriteRule.Condition> conds = (List<FavoriteRule.Condition>) (List<?>) rule.getConds();

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            result.put("freq_enable", rule.isEnabled());
            String frequency = conds.get(0).getRightThreshold();
            if (StringUtils.isNotEmpty(frequency))
                result.put("freq_value", Float.valueOf(frequency));
            else
                result.put("freq_value", null);
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            result.put("count_enable", rule.isEnabled());
            String count = conds.get(0).getRightThreshold();
            if (StringUtils.isNotEmpty(count))
                result.put("count_value", Float.valueOf(count));
            else
                result.put("count_value", null);
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            List<String> users = Lists.newArrayList();
            conds.forEach(cond -> users.add(cond.getRightThreshold()));
            result.put("submitter_enable", rule.isEnabled());
            result.put("users", users);
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            List<String> userGroups = Lists.newArrayList();
            conds.forEach(cond -> userGroups.add(cond.getRightThreshold()));
            result.put("user_groups", userGroups);
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            result.put("duration_enable", rule.isEnabled());
            String minDuration = conds.get(0).getLeftThreshold();
            String maxDuration = conds.get(0).getRightThreshold();

            if (StringUtils.isNotEmpty(minDuration))
                result.put("min_duration", Long.valueOf(minDuration));
            else
                result.put("min_duration", null);

            if (StringUtils.isNotEmpty(maxDuration))
                result.put("max_duration", Long.valueOf(maxDuration));
            else
                result.put("max_duration", null);

            break;
        default:
            break;
        }
    }

    private FavoriteRule getFavoriteRule(String project, String ruleName) {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        Preconditions.checkArgument(ruleName != null && StringUtils.isNotEmpty(ruleName));

        return FavoriteRule.getDefaultRule(getFavoriteRuleManager(project).getByName(ruleName), ruleName);
    }

    @Transaction(project = 0)
    public void updateRegularRule(String project, FavoriteRuleUpdateRequest request) {
        aclEvaluate.checkProjectWritePermission(project);
        favoriteRuleNames.forEach(ruleName -> updateSingleRule(project, ruleName, request));

        NFavoriteScheduler favoriteScheduler = getFavoriteScheduler(project);
        if (!favoriteScheduler.hasStarted()) {
            throw new IllegalStateException("Auto favorite scheduler for " + project + " has not been started");
        }
        favoriteScheduler.scheduleImmediately();
    }

    private void updateSingleRule(String project, String ruleName, FavoriteRuleUpdateRequest request) {
        List<FavoriteRule.Condition> conds = Lists.newArrayList();
        boolean isEnabled = false;

        switch (ruleName) {
        case FavoriteRule.FREQUENCY_RULE_NAME:
            isEnabled = request.isFreqEnable();
            conds.add(new FavoriteRule.Condition(null, request.getFreqValue()));
            break;
        case FavoriteRule.COUNT_RULE_NAME:
            isEnabled = request.isCountEnable();
            conds.add(new FavoriteRule.Condition(null, request.getCountValue()));
            break;
        case FavoriteRule.SUBMITTER_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUsers()))
                request.getUsers().forEach(user -> conds.add(new FavoriteRule.Condition(null, user)));
            break;
        case FavoriteRule.SUBMITTER_GROUP_RULE_NAME:
            isEnabled = request.isSubmitterEnable();
            if (CollectionUtils.isNotEmpty(request.getUserGroups()))
                request.getUserGroups().forEach(userGroup -> conds.add(new FavoriteRule.Condition(null, userGroup)));
            break;
        case FavoriteRule.DURATION_RULE_NAME:
            isEnabled = request.isDurationEnable();
            conds.add(new FavoriteRule.Condition(request.getMinDuration(), request.getMaxDuration()));
            break;
        default:
            break;
        }

        getFavoriteRuleManager(project).updateRule(conds, isEnabled, ruleName);
    }

    @Transaction(project = 0)
    public void batchDeleteFQs(String project, List<String> uuids, boolean block) {
        aclEvaluate.checkProjectWritePermission(project);
        uuids.forEach(uuid -> {
            val favoriteQuery = getFavoriteQueryManager(project).getByUuid(uuid);
            if (favoriteQuery == null)
                throw new BadRequestException(String.format(MsgPicker.getMsg().getFAVORITE_QUERY_NOT_EXIST(), uuid));

            if (block) {
                // put to blacklist
                val sqlCondition = new FavoriteRule.SQLCondition(favoriteQuery.getSqlPattern());
                getFavoriteRuleManager(project).appendSqlPatternToBlacklist(sqlCondition);
            }

            // delete favorite query
            getFavoriteQueryManager(project).delete(favoriteQuery);
        });
    }

    public List<FavoriteRule.SQLCondition> getBlacklistSqls(String project, String sql) {
        FavoriteRule blacklist = getFavoriteRuleManager(project).getByName(FavoriteRule.BLACKLIST_NAME);
        String formattedSql = formatSql(sql);
        return blacklist.getConds().stream().map(cond -> (FavoriteRule.SQLCondition) cond)
                .filter(sqlCondition -> StringUtils.isEmpty(sql)
                        || formatSql(sqlCondition.getSqlPattern()).contains(formattedSql))
                .sorted(Comparator.comparingLong(FavoriteRule.SQLCondition::getCreateTime).reversed())
                .collect(Collectors.toList());
    }

    private String formatSql(String sql) {
        return sql.trim().replaceAll("[\t|\n|\f|\r|\u001C|\u001D|\u001E|\u001F\" \"]+", " ").toUpperCase();
    }

    @Transaction(project = 1)
    public void removeBlacklistSql(String id, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        getFavoriteRuleManager(project).removeSqlPatternFromBlacklist(id);
    }

    public Map<String, SQLValidateResult> batchSqlValidate(List<String> sqls, String project) {
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
        aclEvaluate.checkProjectWritePermission(project);
        Map<String, Object> result = Maps.newHashMap();
        List<String> sqls = Lists.newArrayList();
        List<String> filesParseFailed = Lists.newArrayList();

        // add user info before invoking row-filter and hack-select-star
        QueryContext context = QueryContext.current();
        context.setUsername(AclPermissionUtil.getCurrentUsername());
        context.setGroups(AclPermissionUtil.getCurrentUserGroups());
        context.setHasAdminPermission(AclPermissionUtil.isAdminInProject(project));
        // parse file to sqls
        for (MultipartFile file : files) {
            try {
                sqls.addAll(transformFileToSqls(file, project));
            } catch (Exception ex) {
                logger.error("[UNEXPECTED_THINGS_HAPPENED]Error caught when parsing file {} because {} ",
                        file.getOriginalFilename(), ex);
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

    List<String> transformFileToSqls(MultipartFile file, String project) throws IOException {
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
            String correctedSql = QueryUtil.massageSql(sql, project, 0, 0, DEFAULT_SCHEMA, false);
            sqls.add(correctedSql);
        }

        return sqls;
    }

    public SQLValidateResponse sqlValidate(String project, String sql) {
        String correctedSql = QueryUtil.massageSql(sql, project, 0, 0, DEFAULT_SCHEMA, false);
        // sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(Lists.newArrayList(correctedSql), project);
        SQLValidateResult result = map.get(correctedSql);

        return new SQLValidateResponse(result.isCapable(), result.getSqlAdvices());
    }

    public double getAccelerateRatio(String project) {
        AccelerateRatioManager ratioManager = getAccelerateRatioManager(project);
        AccelerateRatio accelerateRatio = ratioManager.get();
        if (accelerateRatio == null || accelerateRatio.getOverallQueryNum() == 0)
            return 0;

        return accelerateRatio.getNumOfQueryHitIndex() / (double) accelerateRatio.getOverallQueryNum();
    }
}
