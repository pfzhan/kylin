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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.query.util.QueryPatternUtil;
import io.kyligence.kap.rest.response.FavoriteRuleResponse;
import io.kyligence.kap.rest.response.UpdateWhitelistResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;
import io.kyligence.kap.smart.query.validator.AbstractSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;
import io.kyligence.kap.smart.query.validator.SqlSyntaxValidator;

@Component("favoriteRuleService")
public class FavoriteRuleService extends BasicService {

    private static final long MB = 1024 * (long) 1024;
    private static final Set<String> DESIRED_TYPES = new HashSet<>();
    static {
        DESIRED_TYPES.add("txt");
        DESIRED_TYPES.add("sql");
    }
    private static final Set<String> CONTENT_TYPE = new HashSet<>();
    static {
        CONTENT_TYPE.add("text/plain");
        CONTENT_TYPE.add("application/octet-stream");
    }

    private static final int DEFAULT_LIMIT = 500;
    private static final int DEFAULT_OFFSET = 0;
    private static final String DEFAULT_SCHEMA = "default";

    @Autowired
    @Qualifier("favoriteQueryService")
    FavoriteQueryService favoriteQueryService;

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
    public void updateRegularRule(String project, FavoriteRuleUpdateRequest request, String ruleName) throws IOException {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));

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

    @Transaction(project = 1)
    public SQLValidateResult appendSqlToBlacklist(String sql, String project) throws IOException {
        //sql validation
        Map<String, SQLValidateResult> map = sqlValidate(Lists.newArrayList(sql), project);
        SQLValidateResult result = map.get(sql);

        if (!result.isCapable())
            return result;

        //save rule to meta store
        String correctedSql = getMassageSql(sql, project);
        String sqlPattern = QueryPatternUtil.normalizeSQLPattern(correctedSql);
        int sqlPatternHash = sqlPattern.hashCode();

        FavoriteRule.SQLCondition sqlCondition = new FavoriteRule.SQLCondition(sql, sqlPatternHash, result.isCapable());

        try {
            getFavoriteRuleManager(project).appendSqlConditions(Lists.newArrayList(sqlCondition),
                    FavoriteRule.BLACKLIST_NAME);
        } catch (FavoriteRuleManager.RuleConditionExistException ex) {
            throw new IllegalArgumentException(MsgPicker.getMsg().getSQL_ALREADY_IN_WHITELIST());
        }

        getFavoriteQueryManager(project).delete(sqlPattern);
        return result;
    }

    public List<FavoriteRuleResponse> getBlacklistSqls(String project) {
        return getSqlList(project, FavoriteRule.BLACKLIST_NAME);
    }

    @Transaction(project = 1)
    public void removeBlacklistSql(String id, String project) throws IOException {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        getFavoriteRuleManager(project).removeSqlCondition(id, FavoriteRule.BLACKLIST_NAME);
    }

    private Map<String, SQLValidateResult> sqlValidate(List<String> sqls, String project) {
        Map<String, SQLValidateResult> map;
        try {
            AbstractSQLValidator sqlValidator = new SqlSyntaxValidator(getConfig(), project, new MockupQueryExecutor());
            map = sqlValidator.batchValidate(sqls.toArray(new String[sqls.size()]));
        } catch (IOException e) {
            throw new InternalErrorException(MsgPicker.getMsg().getFAIL_TO_VERIFY_SQL(), e);
        }

        return map;
    }

    @Transaction(project = 2)
    public void appendSqlToWhitelist(String sql, int sqlPatternHash, String project) throws IOException {
        FavoriteRule.SQLCondition newSqlCondition = new FavoriteRule.SQLCondition(sql, sqlPatternHash, true);
        try {
            getFavoriteRuleManager(project).appendSqlConditions(Lists.newArrayList(newSqlCondition),
                    FavoriteRule.WHITELIST_NAME);
        } catch (FavoriteRuleManager.RuleConditionExistException ex) {
            throw new IllegalArgumentException(MsgPicker.getMsg().getSQL_ALREADY_IN_BLACKLIST());
        }
    }

    @Transaction(project = 1)
    public void loadSqlsToWhitelist(MultipartFile file, String project) throws IOException {
        List<String> sqls = transferFileToSqls(file);
        if (sqls.isEmpty())
            return;

        //sql validation
        Map<String, SQLValidateResult> map = sqlValidate(sqls, project);

        List<FavoriteRule.SQLCondition> conditions = Lists.newArrayList();
        Set<String> capableSqlPatterns = new HashSet<>();

        for (Map.Entry<String, SQLValidateResult> entry : map.entrySet()) {
            String sql = entry.getKey();
            SQLValidateResult result = entry.getValue();

            String sqlPattern = QueryPatternUtil.normalizeSQLPattern(getMassageSql(sql, project));
            int sqlPatternHash = sqlPattern.hashCode();

            FavoriteRule.SQLCondition newSqlCondition = new FavoriteRule.SQLCondition(sql, sqlPatternHash,
                    result.isCapable());

            if (result.isCapable()) {
                capableSqlPatterns.add(sqlPattern);
            } else {
                Set<FavoriteRule.SQLAdvice> sqlAdvices = new HashSet<>();
                for (SQLAdvice sqlAdvice : result.getSqlAdvices()) {
                    sqlAdvices
                            .add(new FavoriteRule.SQLAdvice(sqlAdvice.getIncapableReason(), sqlAdvice.getSuggestion()));
                }
                newSqlCondition.setSqlAdvices(sqlAdvices);
            }

            conditions.add(newSqlCondition);
        }

        try {
            getFavoriteRuleManager(project).appendSqlConditions(conditions, FavoriteRule.WHITELIST_NAME);
        } catch (FavoriteRuleManager.RuleConditionExistException ex) {
            throw new IllegalArgumentException(MsgPicker.getMsg().getSQL_ALREADY_IN_BLACKLIST());
        }

        // put to favorite query list and accelerate these sqls
        favoriteQueryService.favoriteForWhitelistChannel(capableSqlPatterns, project);
    }

    private List<String> transferFileToSqls(MultipartFile file) {
        Message msg = MsgPicker.getMsg();

        String content = "";
        String contentType = file.getContentType();
        String fileType = FilenameUtils.getExtension(file.getOriginalFilename());
        if (!CONTENT_TYPE.contains(contentType) || file.getSize() >= MB || !DESIRED_TYPES.contains(fileType)) {
            throw new InternalErrorException(msg.getUPLOADED_FILE_TYPE_OR_SIZE_IS_NOT_DESIRED());
        }

        try {
            content = new String(file.getBytes(), "UTF-8");
        } catch (IOException e) {
            throw new InternalErrorException(msg.getFAIL_TO_VERIFY_SQL(), e);
        }
        if (content.isEmpty()) {
            throw new InternalErrorException(msg.getNO_SQL_FOUND());
        }
        content = QueryUtil.removeCommentInSql(content);
        String[] sqlsArray = content.split(";");
        if (sqlsArray == null || sqlsArray.length == 0) {
            throw new InternalErrorException(msg.getNO_SQL_FOUND());
        }
        List<String> sqls = new ArrayList<>();
        for (String sql : sqlsArray) {
            if (sql == null || sql.length() == 0 || sql.replace('\n', ' ').trim().length() == 0) {
                continue;
            }
            sqls.add(sql.replace('\n', ' ').trim());
        }

        return sqls;
    }

    private String getMassageSql(String sql, String project) {
        return QueryUtil.massageSql(sql, project, DEFAULT_LIMIT, DEFAULT_OFFSET, DEFAULT_SCHEMA);
    }

    @Transaction(project = 2)
    public UpdateWhitelistResponse updateWhitelistSql(String updatedSql, String id, String project) throws IOException {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));

        // sql validation
        Map<String, SQLValidateResult> map = sqlValidate(Lists.newArrayList(updatedSql), project);

        SQLValidateResult result = map.get(updatedSql);

        UpdateWhitelistResponse response = new UpdateWhitelistResponse(result.isCapable(), result.getSqlAdvices());

        if (!response.isCapable())
            return response;

        // update to whitelist
        String updatedSqlPattern = QueryPatternUtil.normalizeSQLPattern(getMassageSql(updatedSql, project));
        int updatedSqlPatternHash = updatedSqlPattern.hashCode();

        FavoriteRule.SQLCondition updatedCondition = new FavoriteRule.SQLCondition(updatedSql, updatedSqlPatternHash,
                result.isCapable());
        updatedCondition.setId(id);

        try {
            FavoriteRule.SQLCondition actualUpdatedCondition = getFavoriteRuleManager(project)
                    .updateWhitelistSql(updatedCondition);
            if (actualUpdatedCondition == null)
                throw new IllegalArgumentException(MsgPicker.getMsg().getSQL_IN_WHITELIST_OR_ID_NOT_FOUND());
        } catch (FavoriteRuleManager.RuleConditionExistException ex) {
            throw new IllegalArgumentException(MsgPicker.getMsg().getSQL_ALREADY_IN_BLACKLIST());
        }

        // put to favorite query and accelerate right now
        Set<String> sqlPatternSet = new HashSet<>();
        sqlPatternSet.add(updatedSqlPattern);
        favoriteQueryService.favoriteForWhitelistChannel(sqlPatternSet, project);

        return response;
    }

    private List<FavoriteRuleResponse> getSqlList(String project, String ruleName) {
        List<FavoriteRuleResponse> result = Lists.newArrayList();
        FavoriteRule rule = getFavoriteRule(project, ruleName);

        for (FavoriteRule.AbstractCondition condition : rule.getConds()) {
            FavoriteRule.SQLCondition sqlCondition = (FavoriteRule.SQLCondition) condition;
            FavoriteRuleResponse response = new FavoriteRuleResponse(sqlCondition.getId(), sqlCondition.getSql());
            response.setCapable(sqlCondition.isCapable());
            if (!sqlCondition.isCapable()) {
                response.setSqlAdvices(sqlCondition.getSqlAdvices());
            }
            result.add(response);
        }

        return result;
    }

    public List<FavoriteRuleResponse> getWhitelist(String project) {
        return getSqlList(project, FavoriteRule.WHITELIST_NAME);
    }

    @Transaction(project = 1)
    public void removeWhitelistSql(String id, String project) throws IOException {
        Preconditions.checkArgument(project != null && StringUtils.isNotEmpty(project));
        getFavoriteRuleManager(project).removeSqlCondition(id, FavoriteRule.WHITELIST_NAME);
    }

    public double getFavoriteRuleOverallImpact(String project) {
        // get rule based favorite query size
        int favoriteQuerySize = getFavoriteQueryManager(project).getRuleBasedSize();
        int sqlPatternSizeOfQueryHistory;

        Map<String, Integer> sqlPatternFreqMapInProj = getFavoriteScheduler(project).getOverAllStatus().getSqlPatternFreqMap();

        sqlPatternSizeOfQueryHistory = sqlPatternFreqMapInProj.size();
        if (sqlPatternSizeOfQueryHistory == 0)
            return 0;

        return favoriteQuerySize / (double) sqlPatternSizeOfQueryHistory;
    }
}
