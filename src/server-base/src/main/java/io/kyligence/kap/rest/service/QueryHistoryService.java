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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService {
    private static String localIp;

    public void upsertQueryHistory(SQLRequest sqlRequest, SQLResponse sqlResponse, long startTime) throws IOException {
        String project = sqlRequest.getProject();
        QueryHistory queryHistory = new QueryHistory(QueryContext.current().getQueryId(), sqlRequest.getSql(),
                startTime, sqlResponse.getDuration(), getLocalIP(), Thread.currentThread().getName(),
                sqlRequest.getUsername());

        if (sqlResponse.getColumnMetas() != null) {
            List<String> columns = Lists.newArrayList();
            for (SelectedColumnMeta columnMeta : sqlResponse.getColumnMetas()) {
                columns.add(columnMeta.getName());
            }
            queryHistory.setContent(columns);
        }

        queryHistory.setTotalScanBytes(sqlResponse.getTotalScanBytes());
        queryHistory.setTotalScanCount(sqlResponse.getTotalScanCount());

        if (sqlResponse.getIsException()) {
            queryHistory.setQueryStatus(QueryHistoryStatusEnum.FAILED);
            queryHistory.setAccelerateStatus(QueryHistory.QUERY_HISTORY_UNACCELERATED);
            getQueryHistoryManager(project).save(queryHistory);
            return;
        }

        if (sqlResponse.getResults() != null)
            queryHistory.setResultRowCount(sqlResponse.getResults().size());

        if (sqlResponse.isPushDown()) {
            queryHistory.setRealization(Lists.newArrayList(QueryHistory.ADJ_PUSHDOWN));
            queryHistory.setAccelerateStatus(QueryHistory.QUERY_HISTORY_UNACCELERATED);
        } else {
            queryHistory.setCubeHit(true);

            Set<String> modelNames = new HashSet<>();
            List<String> realization = Lists.newArrayList();
            if (OLAPContext.getThreadLocalContexts() != null) {
                for (final OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                    if (ctx.realization != null) {
                        modelNames.add(ctx.realization.getModel().getName());
                        realization.add(String.valueOf(ctx.storageContext.getCandidate().getCuboidLayout().getId()));
                    }
                }
            }

            queryHistory.setRealization(realization);
            queryHistory.setModelName(modelNames.toString());
            queryHistory.setAccelerateStatus(QueryHistory.QUERY_HISTORY_ACCELERATED);
        }

        queryHistory.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
        getQueryHistoryManager(project).save(queryHistory);
    }

    synchronized private String getLocalIP() throws IOException {
        if (localIp == null) {
            localIp = InetAddress.getLocalHost().getHostAddress();
        }
        return localIp;
    }

    public List<QueryHistory> getQueryHistories(final String project) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        return getQueryHistoryManager(project).getAllQueryHistories();
    }

    private boolean compare(final QueryFilterRule.QueryHistoryCond cond, QueryHistory queryHistory, List<QueryHistory> queryHistories) {
        Object value = null;

        if (cond.getField().equals("frequency")) {
            switch (cond.getOp()) {
                case GREATER:
                    return Collections.frequency(getDailyQueriesSqls(queryHistories), queryHistory.getSql()) > Integer.valueOf(cond.getRightThreshold());
                case LESS:
                    return Collections.frequency(getDailyQueriesSqls(queryHistories), queryHistory.getSql()) < Integer.valueOf(cond.getRightThreshold());
                case EQUAL:
                    return Collections.frequency(getDailyQueriesSqls(queryHistories), queryHistory.getSql()) == Integer.valueOf(cond.getRightThreshold());
                default:
                    throw new IllegalArgumentException("Wrong operation " + cond.getOp());
            }
        }

        try {
            value = queryHistory.getValueByFieldName(cond.getField());
        } catch (Throwable ex) {
            throw new InternalErrorException(ex);
        }

        if (value == null) {
            return false;
        }

        switch (cond.getOp()) {
            case GREATER:
                if (value instanceof Long) {
                    return (long) value > Long.valueOf(cond.getRightThreshold());
                } else
                    throw new IllegalArgumentException("Wrong field type " + value.getClass().getName());
            case LESS:
                if (value instanceof Long) {
                    return (long) value < Long.valueOf(cond.getRightThreshold());
                } else
                    throw new IllegalArgumentException("Wrong field type " + value.getClass().getName());
            case EQUAL:
                if (value instanceof Long) {
                    return (long) value == Long.valueOf(cond.getRightThreshold());
                } else
                    return value.toString().equals(cond.getRightThreshold());
            case CONTAIN:
                return value.toString().contains(cond.getRightThreshold());
            case TO:
                if (value instanceof Long) {
                    return (long) value > Long.valueOf(cond.getLeftThreshold()) && (long) value < Long.valueOf(cond.getRightThreshold());
                } else
                    throw new IllegalArgumentException("Wrong field type " + value.getClass().getName());
            default:
                throw new IllegalArgumentException("Wrong operation " + cond.getOp());
        }
    }

    public List<QueryHistory> getQueryHistoriesByRules(final List<QueryFilterRule> rules, final List<QueryHistory> queryHistories) throws IOException {
        List<Predicate<QueryHistory>> andPredicates = Lists.newArrayList();
        List<Predicate<QueryHistory>> orPredicates = Lists.newArrayList();
        Predicate<QueryHistory> predicate = null;

        if (rules == null || rules.size() == 0)
            return queryHistories;

        for (final QueryFilterRule rule : rules) {
            andPredicates.clear();
            // handle the case when multiple conds filter the same field
            Map<String, List<QueryFilterRule.QueryHistoryCond>> condsMap = Maps.newHashMap();
            for (QueryFilterRule.QueryHistoryCond cond : rule.getConds()) {
                if (!condsMap.containsKey(cond.getField())) {
                    condsMap.put(cond.getField(), Lists.newArrayList(cond));
                } else {
                    List<QueryFilterRule.QueryHistoryCond> conds = condsMap.get(cond.getField());
                    conds.add(cond);
                    condsMap.put(cond.getField(), conds);
                }
            }

            for (final Map.Entry<String, List<QueryFilterRule.QueryHistoryCond>> entry : condsMap.entrySet()) {
                predicate = new Predicate<QueryHistory>() {
                    @Override
                    public boolean apply(@Nullable QueryHistory queryHistory) {
                        boolean result = false;
                        for (QueryFilterRule.QueryHistoryCond cond : entry.getValue()) {
                            result = result || compare(cond, queryHistory, queryHistories);
                            if (result)
                                return result;
                        }

                        return result;
                    }
                };

                if (predicate != null)
                    andPredicates.add(predicate);
            }

            if (andPredicates != null && andPredicates.size() != 0)
                orPredicates.add(Predicates.and(andPredicates));
        }

        if (orPredicates != null && orPredicates.size() != 0) {
            return Lists.newArrayList(Iterables.filter(queryHistories, Predicates.or(orPredicates)));
        } else
            return queryHistories;

    }

    private List<String> getDailyQueriesSqls(List<QueryHistory> queryHistories) {
        List<String> sqls = Lists.newArrayList();
        Calendar now = Calendar.getInstance();
        now.setTime(new Date());
        Calendar queryStartTime = Calendar.getInstance();
        for (QueryHistory queryHistory : queryHistories) {
            queryStartTime.setTime(new Date(queryHistory.getStartTime()));
            if (now.get(Calendar.YEAR) == queryStartTime.get(Calendar.YEAR) && now.get(Calendar.DAY_OF_YEAR) == queryStartTime.get(Calendar.DAY_OF_YEAR)) {
                sqls.add(queryHistory.getSql());
            }
        }

        return sqls;
    }

    public QueryFilterRule parseQueryFilterRuleRequest(long startTimeFrom, long startTimeTo, long latencyFrom, long latencyTo, String sql, List<String> realizations, List<String> accelerateStatuses) {
        List<QueryFilterRule.QueryHistoryCond> conds = Lists.newArrayList();
        if (startTimeFrom != -1 && startTimeTo != -1) {
            QueryFilterRule.QueryHistoryCond cond = new QueryFilterRule.QueryHistoryCond();
            cond.setOp(QueryFilterRule.QueryHistoryCond.Operation.TO);
            cond.setField("startTime");
            cond.setLeftThreshold(String.valueOf(startTimeFrom));
            cond.setRightThreshold(String.valueOf(startTimeTo));
            conds.add(cond);
        }

        if (latencyFrom != -1 && latencyTo != -1) {
            QueryFilterRule.QueryHistoryCond cond = new QueryFilterRule.QueryHistoryCond();
            cond.setOp(QueryFilterRule.QueryHistoryCond.Operation.TO);
            cond.setField("latency");
            cond.setLeftThreshold(String.valueOf(latencyFrom*1000));
            cond.setRightThreshold(String.valueOf(latencyTo*1000));
            conds.add(cond);
        }

        if (sql != null && StringUtils.isNotBlank(sql)) {
            QueryFilterRule.QueryHistoryCond cond = new QueryFilterRule.QueryHistoryCond();
            cond.setOp(QueryFilterRule.QueryHistoryCond.Operation.CONTAIN);
            cond.setField("sql");
            cond.setRightThreshold(sql);
            conds.add(cond);
        }

        if (realizations != null && realizations.size() != 0) {
            for (String realization : realizations) {
                if (realization != null) {
                    QueryFilterRule.QueryHistoryCond cond = new QueryFilterRule.QueryHistoryCond();
                    cond.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
                    cond.setField("isCubeHit");
                    if (realization.equals("pushdown")) {
                        cond.setRightThreshold("false");
                        conds.add(cond);
                    } else if(realization.equals("modelName")) {
                        cond.setRightThreshold("true");
                        conds.add(cond);
                    } else
                        throw new IllegalArgumentException();
                }
            }

        }

        if (accelerateStatuses != null && accelerateStatuses.size() != 0) {
            for (String accelerateStatus : accelerateStatuses) {
                if (accelerateStatus != null) {
                    QueryFilterRule.QueryHistoryCond cond = new QueryFilterRule.QueryHistoryCond();
                    cond.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
                    cond.setField("accelerateStatus");
                    cond.setRightThreshold(accelerateStatus);
                    conds.add(cond);
                }
            }

        }

        if (conds.size() == 0)
            return null;

        QueryFilterRule rule = new QueryFilterRule();
        rule.setConds(conds);
        return rule;
    }
}
