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

package io.kyligence.kap.smart.model.rule;

import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.query.util.QueryAliasMatchInfo;
import io.kyligence.kap.query.util.QueryAliasMatcher;
import io.kyligence.kap.query.util.SqlSubqueryFinder;
import io.kyligence.kap.smart.common.SmartConfig;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CCRuleAdvisor {

    // configurable
    private static ImmutableList<IAdviceRule> rules = ImmutableList.copyOf(initCCRules());

    public List<String> suggestCandidate(String project, NDataModel modelDesc, String sql) {
        TableDesc tableDesc = modelDesc.getRootFactTable().getTableDesc();
        String schemaName = tableDesc.getDatabase();
        String tableName = tableDesc.getName();

        List<SqlCall> selectOrOrderbys;
        try {
            selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
        } catch (SqlParseException e) {
            log.warn("Advice CC failed, error in visiting subqueries, {}", e.getMessage());
            return Lists.newArrayList();
        }

        QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher(project, schemaName);
        List<String> ccSuggestions = Lists.newArrayList();
        for (SqlCall selectOrOrderby : selectOrOrderbys) {
            // Parse query to locate CC expression
            SqlSelect sqlSelect = KapQueryUtil.extractSqlSelect(selectOrOrderby);
            if (sqlSelect == null) {
                continue;
            }
            QueryAliasMatchInfo info = null;
            try {
                info = queryAliasMatcher.match(modelDesc, sqlSelect);
            } catch (Exception e) {
                log.warn("Advice CC failed, error in analyzing query alias, {}, {}", sqlSelect, e.getMessage());
            }
            if (info == null) {
                // Skip parent query if not directly access table
                continue;
            }

            List<String> blackList = Lists.newArrayList();
            CCRuleVisitor ruleVisitor = new CCRuleVisitor();
            // extract CC expressions from aggregations in SqlSelect
            extractCCSuggestionByAggFunctionRule(sqlSelect, blackList, ruleVisitor);

            // extract CC expressions from groupBy simple caseWhen Clause
            extractCCSuggestionWithCaseWhenRule(sqlSelect, ruleVisitor);

            for (String suggestion : ruleVisitor.getSuggestions()) {
                if (isSuggestionInBlackList(suggestion, blackList)) {
                    continue;
                }
                String expr = suggestion;
                try {
                    if (CalciteParser.hasAliasInExpr(expr)) {
                        expr = CalciteParser.replaceAliasInExpr(expr, info.getAliasMapping());
                    } else {
                        // Use root table as default table alias
                        expr = CalciteParser.insertAliasInExpr(expr, tableName);
                    }
                } catch (Exception e) {
                    log.warn("Cannot resolve table alias of {}", suggestion, e);
                }
                ccSuggestions.add(expr);
            }
        }

        return ccSuggestions;
    }

    private void extractCCSuggestionByAggFunctionRule(SqlSelect sqlSelect, List<String> blackList,
            CCRuleVisitor ruleVisitor) {
        KapQueryUtil.collectSelectList(sqlSelect).stream() //
                .filter(sqlNode -> {
                    val sqlAggFunctionVisitor = new AggregationDetector();
                    sqlNode.accept(sqlAggFunctionVisitor);
                    blackList.addAll(sqlAggFunctionVisitor.getBlackList());
                    return sqlAggFunctionVisitor.isEffectiveAgg();
                }).forEach(sqlNode -> sqlNode.accept(ruleVisitor));
    }

    private void extractCCSuggestionWithCaseWhenRule(SqlSelect sqlSelect, CCRuleVisitor ruleVisitor) {
        KapQueryUtil.collectGroupByNodes(sqlSelect).stream() //
                .filter(sqlNode -> {
                    boolean valid = false;
                    if (sqlNode instanceof SqlCase) {
                        val visitor = new CaseWhenDetector();
                        sqlNode.accept(visitor);
                        valid = !visitor.isComplex();
                    }
                    return valid;
                }).forEach(node -> node.accept(ruleVisitor));
    }

    private boolean isSuggestionInBlackList(String ccSuggestion, List<String> blackList) {
        for (String str : blackList) {
            if (str.contains(ccSuggestion)) {
                return true;
            }
        }
        return false;
    }

    @Slf4j
    static class CCRuleVisitor extends SqlBasicVisitor<Object> implements IKeep {

        @Getter
        private Set<String> suggestions = Sets.newHashSet();

        @Override
        public Object visit(SqlIdentifier id) {
            return null;
        }

        @Override
        public Object visit(SqlCall call) {
            for (IAdviceRule rule : rules) {
                String ccExp = rule.matches(call);
                if (StringUtils.isNotEmpty(ccExp)) {
                    suggestions.add(ccExp);
                    log.trace("CC Exp({}) generated by rule {}", ccExp, rule.getClass().getName());
                    return null;
                }
            }
            return call.getOperator().acceptCall(this, call);
        }
    }

    private static Set<IAdviceRule> initCCRules() {
        SmartConfig config = SmartConfig.getInstanceFromEnv();
        String[] ruleClassNames = config.getSpecialCCRulesOnSqlNode();
        Set<IAdviceRule> rules = Sets.newHashSet();
        for (String ruleClassName : ruleClassNames) {
            IAdviceRule rule = (IAdviceRule) ClassUtil.newInstance(ruleClassName);
            rules.add(rule);
        }
        return rules;
    }
}
