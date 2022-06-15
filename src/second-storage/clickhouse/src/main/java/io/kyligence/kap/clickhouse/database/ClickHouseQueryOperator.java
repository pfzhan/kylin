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

package io.kyligence.kap.clickhouse.database;

import com.google.common.collect.ImmutableMap;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseSystemQuery;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.database.QueryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ClickHouseQueryOperator implements QueryOperator {
    private final String project;

    public ClickHouseQueryOperator(String project) {
        this.project = project;
    }

    public Map<String, Object> getQueryMetric(String queryId) {
        if (!SecondStorageUtil.isProjectEnable(project) || StringUtils.isEmpty(queryId)) {
            return exceptionQueryMetric();
        }

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Set<String>> allShards = SecondStorageNodeHelper.groupsToShards(SecondStorageUtil.listNodeGroup(config, project));

        // filter if one node down
        List<Set<String>> upShards = allShards.stream()
                .map(replicas -> replicas.stream().filter(SecondStorageQueryRouteUtil::getNodeStatus).collect(Collectors.toSet()))
                .filter(replicas -> !replicas.isEmpty()).collect(Collectors.toList());

        if (upShards.size() != allShards.size()) {
            return exceptionQueryMetric();
        }

        String sql = ClickHouseSystemQuery.queryQueryMetric(queryId);
        Map<String, ClickHouseSystemQuery.QueryMetric> queryMetricMap = upShards.parallelStream().flatMap(replicas -> {
            Map<String, ClickHouseSystemQuery.QueryMetric> queryMetrics = new HashMap<>();
            try {
                queryMetrics.putAll(getQueryMetric(replicas, sql, false));

                if (!queryMetrics.isEmpty()) {
                    return queryMetrics.entrySet().stream();
                }

                queryMetrics.putAll(getQueryMetric(replicas, sql, true));
            } catch (SQLException ex) {
                log.error("Fetch tired storage query metric fail.", ex);
                queryMetrics.put(queryId, ClickHouseSystemQuery.QueryMetric.builder().clientName(queryId).readBytes(-1).readRows(-1).build());
            }

            return queryMetrics.entrySet().stream();
        }).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (o1, o2) -> {
                    if (queryId.equals(o1.getClientName())) {
                        return o1;
                    }

                    if (queryId.equals(o2.getClientName())) {
                        return o2;
                    }

                    o1.setReadBytes(o2.getReadBytes() + o1.getReadBytes());
                    o1.setReadRows(o2.getReadRows() + o1.getReadRows());
                    return o1;
                }
        ));

        if (queryMetricMap.containsKey(queryId)) {
            return exceptionQueryMetric();
        }

        Optional<String> lastQueryIdOptional = queryMetricMap.keySet().stream().filter(q -> !queryId.equals(q)).max(Comparator.comparing(String::toString));

        if (!lastQueryIdOptional.isPresent()) {
            return exceptionQueryMetric();
        }

        String lastQueryId = lastQueryIdOptional.get();
        return ImmutableMap.of(QueryMetrics.TOTAL_SCAN_COUNT, queryMetricMap.get(lastQueryId).getReadRows(), QueryMetrics.TOTAL_SCAN_BYTES, queryMetricMap.get(lastQueryId).getReadBytes());
    }

    public Map<String, ClickHouseSystemQuery.QueryMetric> getQueryMetric(Set<String> replicas, String sql, boolean needFlush) throws SQLException {
        Map<String, ClickHouseSystemQuery.QueryMetric> queryMetrics = new HashMap<>(3);
        for (String replica : replicas) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(replica))) {
                if (needFlush) {
                    clickHouse.apply("SYSTEM FLUSH LOGS");
                }

                List<ClickHouseSystemQuery.QueryMetric> result = clickHouse.query(sql, ClickHouseSystemQuery.QUERY_METRIC_MAPPER);

                for (ClickHouseSystemQuery.QueryMetric queryMetric : result) {
                    queryMetrics.put(queryMetric.getClientName(), queryMetric);
                }
            }
        }

        return queryMetrics;
    }

    private Map<String, Object> exceptionQueryMetric() {
        return ImmutableMap.of(QueryMetrics.TOTAL_SCAN_COUNT, -1L, QueryMetrics.TOTAL_SCAN_BYTES, -1L);
    }

}
