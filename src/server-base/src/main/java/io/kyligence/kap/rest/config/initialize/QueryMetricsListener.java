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

package io.kyligence.kap.rest.config.initialize;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.constant.Constant;
import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetrics;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.QueryMetricsContext;
import lombok.val;

public class QueryMetricsListener {

    @Subscribe
    public void recordMetric(QueryMetrics queryMetric) {
        String project = queryMetric.getProjectName();
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), queryMetric.getServer().concat("-").concat(project));

        MetricsGroup.counterInc(MetricsName.QUERY, MetricsCategory.PROJECT, project, tags);

        updateQueryTimeMetrics(queryMetric.getQueryDuration(), project, tags);
        updateQueryTypeMetrics(queryMetric, project, tags);

        MetricsGroup.counterInc(MetricsName.QUERY_HOST, MetricsCategory.HOST, queryMetric.getServer());
        MetricsGroup.counterInc(MetricsName.QUERY_SCAN_BYTES_HOST, MetricsCategory.HOST, queryMetric.getServer(),
                queryMetric.getTotalScanBytes());

        MetricsGroup.histogramUpdate(MetricsName.QUERY_LATENCY, MetricsCategory.PROJECT, queryMetric.getProjectName(),
                tags, queryMetric.getQueryDuration());
        MetricsGroup.histogramUpdate(MetricsName.QUERY_TIME_HOST, MetricsCategory.HOST, queryMetric.getServer(),
                queryMetric.getQueryDuration());

        MetricsGroup.histogramUpdate(MetricsName.QUERY_SCAN_BYTES, MetricsCategory.PROJECT, project, tags,
                queryMetric.getTotalScanBytes());

        PrometheusMetricsGroup.summaryRecord(queryMetric.getQueryDuration() * 1.0 / 1000,
                PrometheusMetrics.QUERY_SECONDS, new double[] { 0.8, 0.9 }, //
                "pushdown", queryMetric.isPushdown() + "", //
                "cache", queryMetric.isCacheHit() + "", "hit_index", queryMetric.isIndexHit() + "", //
                "hit_exactly_index", queryMetric.getQueryHistoryInfo().isExactlyMatch() + "", //
                "succeed", queryMetric.isSucceed() + "", "hit_snapshot", queryMetric.isTableSnapshotUsed() + "", //
                "instance", queryMetric.getServer(), "project", queryMetric.getProjectName());

        // only record scan bytes when hit index
        if (queryMetric.isIndexHit()) {
            PrometheusMetricsGroup.summaryRecord(queryMetric.getTotalScanBytes(), //
                    PrometheusMetrics.QUERY_SCAN_BYTES, new double[] { 0.8, 0.9 }, //
                    "project", project, "model",
                    queryMetric.getRealizationMetrics().stream()
                            .map(m -> modelManager.getDataModelDesc(m.getModelId()).getAlias())
                            .collect(Collectors.joining(",")),
                    "instance", queryMetric.getServer());
        }

    }

    private void updateQueryTypeMetrics(QueryMetrics queryMetrics, String project, Map<String, String> tags) {
        if (queryMetrics.getErrorType() != null) {
            MetricsGroup.counterInc(MetricsName.QUERY_FAILED, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_FAILED_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.isPushdown()) {
            MetricsGroup.counterInc(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_PUSH_DOWN_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.isTimeout()) {
            MetricsGroup.counterInc(MetricsName.QUERY_TIMEOUT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_TIMEOUT_RATE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.isCacheHit()) {
            MetricsGroup.counterInc(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, project, tags);
        }

        if (queryMetrics.getRealizationMetrics() != null) {
            boolean hitAggIndex = queryMetrics.getRealizationMetrics().stream()
                    .anyMatch(realization -> realization != null
                            && QueryMetricsContext.AGG_INDEX.equals(realization.getIndexType()));
            boolean hitTableIndex = queryMetrics.getRealizationMetrics().stream()
                    .anyMatch(realization -> realization != null
                            && QueryMetricsContext.TABLE_INDEX.equals(realization.getIndexType()));
            if (hitAggIndex) {
                MetricsGroup.counterInc(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, project, tags);
            }
            if (hitTableIndex) {
                MetricsGroup.counterInc(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, project, tags);
            }
        }
    }

    @VisibleForTesting
    public void updateQueryTimeMetrics(long duration, String project, Map<String, String> tags) {
        if (duration <= Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 3 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 5 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, project, tags);
        } else if (duration <= 10 * Constant.SECOND) {
            MetricsGroup.counterInc(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, project, tags);
        } else {
            MetricsGroup.counterInc(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.meterMark(MetricsName.QUERY_SLOW_RATE, MetricsCategory.PROJECT, project, tags);
        }
        MetricsGroup.counterInc(MetricsName.QUERY_TOTAL_DURATION, MetricsCategory.PROJECT, project, tags, duration);
    }

}
