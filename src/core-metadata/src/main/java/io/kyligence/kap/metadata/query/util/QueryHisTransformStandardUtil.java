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

package io.kyligence.kap.metadata.query.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryHistoryInfoResponse;
import io.kyligence.kap.metadata.query.QueryHistoryResponse;
import io.kyligence.kap.metadata.query.QueryRealization;

public class QueryHisTransformStandardUtil {
    public static Map<String, Object> transformQueryHistory(Map<String, Object> queryHistories) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("size", queryHistories.get("size"));
        List<QueryHistoryResponse> queryHistoryResponses = Lists.newArrayList();
        if (queryHistories.get("query_histories") == null) {
            return data;
        }
        List<QueryHistory> queryHistoriesList = (List<QueryHistory>)queryHistories.get("query_histories");
        for (QueryHistory qh : queryHistoriesList) {
            QueryHistoryResponse history = new QueryHistoryResponse();
            history.setQueryRealizations(qh.getQueryRealizations());
            history.setSql(qh.getSql());
            history.setQueryTime(qh.getQueryTime());
            history.setDuration(qh.getDuration());
            history.setHostName(qh.getHostName());
            history.setQuerySubmitter(qh.getQuerySubmitter());
            history.setQueryStatus(qh.getQueryStatus());
            history.setQueryId(qh.getQueryId());
            history.setId(qh.getId());
            history.setTotalScanCount(qh.getTotalScanCount());
            history.setTotalScanBytes(qh.getTotalScanBytes());
            history.setResultRowCount(qh.getResultRowCount());
            history.setCacheHit(qh.isCacheHit());
            history.setIndexHit(qh.isIndexHit());
            history.setEngineType(qh.getEngineType());
            history.setProjectName(qh.getProjectName());
            history.setErrorType(qh.getErrorType());
            history.setNativeQueryRealizations(transformQueryHistoryRealization(qh.getNativeQueryRealizations()));
            history.setQueryHistoryInfo(transformQueryHisInfo(qh.getQueryHistoryInfo()));
            queryHistoryResponses.add(history);
        }
        data.put("query_histories", queryHistoryResponses);
        return data;
    }

    public static List<QueryRealization> transformQueryHistoryRealization(List<NativeQueryRealization> realizations) {
        List<QueryRealization> queryRealizations = Lists.newArrayList();
        if (realizations != null) {
            for (NativeQueryRealization r : realizations) {
                QueryRealization qr = new QueryRealization(
                        r.getModelId(), r.getModelAlias(), r.getLayoutId(), r.getIndexType(),
                        r.isPartialMatchModel(), r.isValid(), r.getSnapshots());
                queryRealizations.add(qr);
            }
        }
        return queryRealizations;
    }

    public static QueryHistoryInfoResponse transformQueryHisInfo(QueryHistoryInfo qh) {
        if (qh == null) {
            return null;
        }
        QueryHistoryInfoResponse queryHistoryInfoResponse = new QueryHistoryInfoResponse(
                qh.isExactlyMatch(), qh.getScanSegmentNum(), qh.getState(), qh.isExecutionError(), qh.getErrorMsg(),
                qh.getQuerySnapshots(), qh.getRealizationMetrics(), qh.getTraces());
        return queryHistoryInfoResponse;
    }
}
