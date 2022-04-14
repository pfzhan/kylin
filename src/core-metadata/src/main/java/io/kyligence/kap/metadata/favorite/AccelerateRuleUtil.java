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

package io.kyligence.kap.metadata.favorite;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import lombok.var;

public class AccelerateRuleUtil {

    static class InternalBlackOutRule {

        private static InternalBlackOutRule instance;

        public static synchronized InternalBlackOutRule getSingletonInstance() {
            if (instance == null) {
                instance = new InternalBlackOutRule();
            }

            return instance;
        }

        private InternalBlackOutRule() {

        }

        /**
         * TRUE --> reserve query history
         * FALSE --> discard query history
         *
         * @param queryHistory
         * @return
         */
        public boolean filterCannotAccelerate(QueryHistory queryHistory) {
            return !exactlyMatchedQuery(queryHistory) && !pushdownForExecutionError(queryHistory);
        }

        private boolean exactlyMatchedQuery(QueryHistory queryHistory) {
            return queryHistory.getQueryHistoryInfo().isExactlyMatch();
        }

        private boolean pushdownForExecutionError(QueryHistory queryHistory) {
            return queryHistory.getQueryHistoryInfo().isExecutionError();
        }
    }

    public List<QueryHistory> findMatchedCandidate(String project, List<QueryHistory> queryHistories, Map<String,
            Set<String>> submitterToGroups, List<Pair<Long, QueryHistoryInfo>> batchArgs) {
        List<QueryHistory> candidate = Lists.newArrayList();
        for (QueryHistory qh : queryHistories) {
            QueryHistoryInfo queryHistoryInfo = qh.getQueryHistoryInfo();
            if (queryHistoryInfo == null) {
                continue;
            }
            if (matchCustomerRule(qh, project, submitterToGroups) && matchInternalRule(qh)) {
                queryHistoryInfo.setState(QueryHistoryInfo.HistoryState.SUCCESS);
                candidate.add(qh);
            } else {
                queryHistoryInfo.setState(QueryHistoryInfo.HistoryState.FAILED);
            }
            batchArgs.add(new Pair<>(qh.getId(), queryHistoryInfo));
        }
        return candidate;
    }

    private boolean matchInternalRule(QueryHistory queryHistory) {
        if (queryHistory.getQueryHistoryInfo() == null) {
            return false;
        }
        return InternalBlackOutRule.getSingletonInstance().filterCannotAccelerate(queryHistory);
    }

    boolean matchCustomerRule(QueryHistory queryHistory, String project, Map<String, Set<String>> submitterToGroups) {
        var submitterRule = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getOrDefaultByName(FavoriteRule.SUBMITTER_RULE_NAME);
        boolean submitterMatch = matchRule(queryHistory, submitterRule,
                (queryHistory1, conditions) -> conditions.stream().anyMatch(cond -> queryHistory1.getQuerySubmitter()
                        .equals(((FavoriteRule.Condition) cond).getRightThreshold())));

        var groupRule = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getOrDefaultByName(FavoriteRule.SUBMITTER_GROUP_RULE_NAME);

        boolean userGroupMatch = matchRule(queryHistory, groupRule,
                (queryHistory1, conditions) -> conditions.stream()
                        .anyMatch(cond -> submitterToGroups.computeIfAbsent(queryHistory1.getQuerySubmitter(),
                                        key -> new HashSet<>())
                                .contains(((FavoriteRule.Condition) cond).getRightThreshold())));

        var durationRule = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getOrDefaultByName(FavoriteRule.DURATION_RULE_NAME);

        boolean durationMatch = matchRule(queryHistory, durationRule,
                (queryHistory1, conditions) -> conditions.stream().anyMatch(cond -> (queryHistory1
                        .getDuration() >= Long.parseLong(((FavoriteRule.Condition) cond).getLeftThreshold()) * 1000L
                        && queryHistory1.getDuration() <= Long
                                .parseLong(((FavoriteRule.Condition) cond).getRightThreshold()) * 1000L)));

        return submitterMatch || userGroupMatch || durationMatch;
    }

    private boolean matchRule(QueryHistory history, FavoriteRule favoriteRule,
            BiPredicate<QueryHistory, List<?>> function) {
        if (!favoriteRule.isEnabled())
            return false;

        return function.test(history, favoriteRule.getConds());
    }

}
