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
import io.kyligence.kap.common.metric.MetricWriter;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.rest.request.QueryHistoryRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService {

    private QueryHistoryManager queryHistoryManager = QueryHistoryManager.getInstance(getConfig());

    private void checkMetricWriterType() {
        if (!MetricWriter.Type.INFLUX.name().equals(KapConfig.wrap(getConfig()).diagnosisMetricWriterType())) {
            throw new IllegalStateException(MsgPicker.getMsg().getNOT_SET_INFLUXDB());
        }
    }

    public List<QueryHistory> getQueryHistories(QueryHistoryRequest request, final int limit, final int offset) {
        Preconditions.checkArgument(request.getProject() != null && StringUtils.isNotEmpty(request.getProject()));
        checkMetricWriterType();

        final String query = getQueryHistorySql(request, limit, offset);
        return queryHistoryManager.getQueryHistoriesBySql(query, QueryHistory.class);
    }

    private String getQueryHistorySql(QueryHistoryRequest request, final int limit, final int offset) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("SELECT * FROM %s WHERE project = '%s' ", QueryHistory.QUERY_MEASUREMENT,
                request.getProject()));

        // filter by time
        sb.append(String.format("AND (query_time >= %d AND query_time < %d) ", request.getStartTimeFrom(),
                request.getStartTimeTo()));
        // filter by duration
        sb.append(String.format("AND (\"duration\" >= %d AND \"duration\" < %d) ", request.getLatencyFrom() * 1000L,
                request.getLatencyTo() * 1000L));

        if (StringUtils.isNotEmpty(request.getSql())) {
            sb.append(String.format("AND sql_text =~ /%s/ ", request.getSql()));
        }

        if (request.getRealizations() != null && !request.getRealizations().isEmpty()) {
            sb.append("AND (");
            for (int i = 0; i < request.getRealizations().size(); i++) {
                switch (request.getRealizations().get(i)) {
                case "pushdown":
                    sb.append("cube_hit = 'false' OR ");
                    break;
                case "modelName":
                    sb.append("cube_hit = 'true' OR ");
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Illegal realization type %s", request.getRealizations().get(i)));
                }

                if (i == request.getRealizations().size() - 1) {
                    sb.setLength(sb.length() - 4);
                    sb.append(") ");
                }
            }
        }

        if (request.getAccelerateStatuses() != null && !request.getAccelerateStatuses().isEmpty()) {
            sb.append("AND (");
            for (int i = 0; i < request.getAccelerateStatuses().size(); i++) {
                if (i == request.getAccelerateStatuses().size() - 1)
                    sb.append(String.format("accelerate_status = '%s') ", request.getAccelerateStatuses().get(i)));
                else
                    sb.append(String.format("accelerate_status = '%s' OR ", request.getAccelerateStatuses().get(i)));
            }
        }

        sb.append(String.format("ORDER BY time DESC LIMIT %d OFFSET %d", limit, offset));
        return sb.toString();
    }

    public List<QueryHistory> getQueryHistories(long startTime, long endTime) {
        checkMetricWriterType();

        String sql = String.format("SELECT * FROM %s WHERE time >= %dms AND time < %dms",
                QueryHistory.QUERY_MEASUREMENT, startTime, endTime);

        return queryHistoryManager.getQueryHistoriesBySql(sql, QueryHistory.class);
    }
}
