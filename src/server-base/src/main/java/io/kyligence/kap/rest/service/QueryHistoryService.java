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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryFilterRule;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
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
            getQueryHistoryManager(project).upsertEntry(queryHistory);
            throw new InternalErrorException(sqlResponse.getExceptionMessage());
        }

        if (sqlResponse.getResults() != null)
            queryHistory.setResultRowCount(sqlResponse.getResults().size());

        if (sqlResponse.isPushDown()) {
            queryHistory.setRealization(QueryHistory.ADJ_PUSHDOWN);
        } else {
            queryHistory.setCubeHit(true);

            Set<String> modelNames = new HashSet<>();

            if (OLAPContext.getThreadLocalContexts() != null) {
                for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                    if (ctx.realization != null) {
                        modelNames.add(ctx.realization.getModel().getName());
                    }
                }
            }

            //TODO: get more detailed realization info
            queryHistory.setRealization(modelNames.toString());
            queryHistory.setModelName(modelNames.toString());
        }

        queryHistory.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
        getQueryHistoryManager(project).upsertEntry(queryHistory);
    }

    private String getLocalIP() throws IOException {
        if (localIp == null) {
            localIp = InetAddress.getLocalHost().getHostAddress();
        }
        return localIp;
    }

    public List<QueryHistory> getQueryHistories(final String project) throws IOException {
        Preconditions.checkArgument(project != null && !StringUtils.isEmpty(project));
        return getQueryHistoryManager(project).getAllQueryHistories();
    }

    public List<QueryHistory> getQueryHistoriesByRules(final String project, final QueryHistoryFilterRule rules) throws IOException {
        List<QueryHistory> allQueryHistories = getQueryHistories(project);
        List<Predicate<QueryHistory>> predicates = Lists.newArrayList();

        if (rules == null)
            return allQueryHistories;

        for (final QueryHistoryFilterRule.QueryHistoryCond cond : rules.getConds()) {
            Predicate<QueryHistory> predicate = new Predicate<QueryHistory>() {
                @Override
                public boolean apply(@Nullable QueryHistory queryHistory) {
                    Object value = null;
                    try {
                        value = queryHistory.getValueByFieldName(cond.getField());
                    } catch (Throwable ex) {
                        throw new InternalErrorException(ex);
                    }

                    if (value == null){
                        throw new InternalErrorException("Can not find field with name as" + cond.getField());
                    }

                    switch (cond.getOp()) {
                        case GREATER:
                            if (value instanceof Float) {
                                return (float) value > Float.valueOf(cond.getThreshold());
                            } else if (value instanceof Long) {
                                return (long) value > Long.valueOf(cond.getThreshold());
                            } else
                                throw new InternalErrorException("Wrong field type " + value.getClass().getName());
                        case LESS:
                            if (value instanceof Float) {
                                return (float) value < Float.valueOf(cond.getThreshold());
                            } else if (value instanceof Long) {
                                return (long) value < Long.valueOf(cond.getThreshold());
                            } else
                                throw new InternalErrorException("Wrong field type " + value.getClass().getName());
                        case EQUAL:
                            if (value instanceof Float) {
                                return (float) value == Float.valueOf(cond.getThreshold());
                            } else
                                return value.toString().equals(cond.getThreshold());
                        case CONTAIN:
                            return value.toString().contains(cond.getThreshold());
                        default:
                            throw new BadRequestException("Wrong operation " + cond.getOp());
                    }
                }
            };

            if (predicate != null)
                predicates.add(predicate);
        }

        List<QueryHistory> filteredQueries = Lists.newArrayList(Iterables.filter(allQueryHistories, Predicates.and(predicates)));

        return filteredQueries;
    }
}
