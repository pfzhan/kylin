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

package io.kyligence.kap.smart.model.cc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.query.util.QueryAliasMatchInfo;
import io.kyligence.kap.query.util.QueryAliasMatcher;
import io.kyligence.kap.query.util.SqlSubqueryFinder;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ComputedColumnAdvisor {

    private static Logger LOGGER = LoggerFactory.getLogger(ComputedColumnAdvisor.class);

    private static final IAdviceRule[] registeredRules = new IAdviceRule[] { 
            new AggFuncRule(),       // SUM({EXPR}): advice complicated EXPR as CC expect SUM(COL) and SUM(NUM) cases
            new ArrayItemRule(),    // array[{INDEX}]: access array item should use CC
            new CaseWhenRule()      // CASE .. WHEN .. 
    };

    public List<String> suggestCandidate(String project, NDataModel modelDesc, String sql) {

        TableDesc tableDesc = modelDesc.getRootFactTable().getTableDesc();
        String schemaName = tableDesc.getDatabase();
        String tableName = tableDesc.getName();

        List<SqlCall> selectOrOrderbys;
        try {
            selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql);
        } catch (SqlParseException e) {
            LOGGER.warn("Advice CC failed, error in visiting subqueries, {}", e.getMessage());
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
                LOGGER.warn("Advice CC failed, error in analyzing query alias, {}, {}", sqlSelect, e.getMessage());
            }
            if (info == null) {
                // Skip parent query if not directly access table
                continue;
            }

            // Get suggested CC expressions
            CCRuleVisitor ruleVisitor = new CCRuleVisitor();
            sqlSelect.accept(ruleVisitor);
            for (String suggestion : ruleVisitor.getSuggestions()) {
                String expr = suggestion;
                try {
                    if (CalciteParser.hasAliasInExpr(expr)) {
                        expr = CalciteParser.replaceAliasInExpr(expr, info.getAliasMapping());
                    } else {
                        // Use root table as default table alias
                        expr = CalciteParser.insertAliasInExpr(expr, tableName);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Cannot resolve table alias of {}", suggestion, e);
                }
                ccSuggestions.add(expr);
            }
        }

        return ccSuggestions;
    }

    static class CCRuleVisitor extends SqlBasicVisitor {
        private Set<String> suggestions = new HashSet<>();

        public Set<String> getSuggestions() {
            return this.suggestions;
        }

        @Override
        public Object visit(SqlIdentifier id) {
            return null;
        }

        @Override
        public Object visit(SqlCall call) {
            for (IAdviceRule rule : registeredRules) {
                String suggestedCC = rule.matches(call);
                if (StringUtils.isNotEmpty(suggestedCC)) {
                    suggestions.add(suggestedCC);
                    return null;
                }
            }
            return call.getOperator().acceptCall(this, call);
        }
    }
}
