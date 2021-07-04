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

package io.kyligence.kap.common.metrics;

import static java.util.stream.Collectors.toMap;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.gauges.QueryRatioGauge;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;

public class MetricsGroup {

    private static final Logger logger = LoggerFactory.getLogger(MetricsGroup.class);

    private static final Set<String> gauges = Collections.synchronizedSet(new HashSet<>());

    private static final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap();

    private static final ConcurrentHashMap<String, Meter> meters = new ConcurrentHashMap();

    private static final ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap();

    private MetricsGroup() {
    }

    public static boolean hostTagCounterInc(MetricsName name, MetricsCategory category, String entity) {
        return counterInc(name, category, entity, getHostTagMap(entity));
    }

    public static boolean hostTagCounterInc(MetricsName name, MetricsCategory category, String entity,
            long increments) {
        return counterInc(name, category, entity, getHostTagMap(entity), increments);
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity) {
        return counterInc(name, category, entity, Collections.emptyMap());
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        return counterInc(name, category, entity, tags, 1);
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity, long increments) {
        return counterInc(name, category, entity, Collections.emptyMap(), increments);
    }

    public static boolean counterInc(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags, long increments) {
        if (increments < 0) {
            return false;
        }
        try {
            final Counter counter = registerCounterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (counter != null) {
                counter.inc(increments);
                return true;
            }
        } catch (Exception e) {
            logger.warn("ke.metrics counterInc {}", e.getMessage());
        }
        return false;
    }

    public static boolean hostTagHistogramUpdate(MetricsName name, MetricsCategory category, String entity,
            long updateTo) {
        return histogramUpdate(name, category, entity, getHostTagMap(entity), updateTo);
    }

    public static boolean histogramUpdate(MetricsName name, MetricsCategory category, String entity, long updateTo) {
        return histogramUpdate(name, category, entity, Collections.emptyMap(), updateTo);
    }

    public static boolean histogramUpdate(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags, long updateTo) {
        if (updateTo < 0) {
            return false;
        }
        try {
            final Histogram histogram = registerHistogramIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (histogram != null) {
                histogram.update(updateTo);
                return true;
            }
        } catch (Exception e) {
            logger.warn("ke.metrics histogramUpdate {}", e.getMessage());
        }
        return false;
    }

    public static boolean meterMark(MetricsName name, MetricsCategory category, String entity) {
        return meterMark(name, category, entity, Collections.emptyMap());
    }

    public static boolean meterMark(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Meter meter = registerMeterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (meter != null) {
                meter.mark();
                return true;
            }
        } catch (Exception e) {
            logger.warn("ke.metrics meterMark {}", e.getMessage());
        }
        return false;
    }

    public static boolean removeGlobalMetrics() {
        try {
            final String metricNameSuffix = metricNameSuffix(MetricsCategory.GLOBAL.getVal(), "global",
                    Collections.emptyMap());
            final MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
            removeMetrics(metricNameSuffix, registry);
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics removeGlobalMetrics {}", e.getMessage());
        }
        return false;
    }

    public static boolean removeProjectMetrics(final String projectName) {
        try {
            if (StringUtils.isEmpty(projectName)) {
                throw new IllegalArgumentException("removeProjectMetrics, projectName shouldn't be empty.");
            }
            final String metricNameSuffix = metricNameSuffix(MetricsCategory.PROJECT.getVal(), projectName,
                    Collections.emptyMap());

            final MetricRegistry registry = MetricsController.getDefaultMetricRegistry();
            removeMetrics(metricNameSuffix, registry);
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics removeProjectMetrics, projectName: {} {}", projectName, e.getMessage());
        }
        return false;
    }

    public static boolean removeModelMetrics(String project, String modelId) {
        try {
            if (StringUtils.isEmpty(project)) {
                throw new IllegalArgumentException("removeModelMetrics, projectName shouldn't be empty.");
            }
            if (StringUtils.isEmpty(modelId)) {
                throw new IllegalArgumentException("removeModelMetrics, modelId shouldn't be empty.");
            }
            Map<String, String> tags = Maps.newHashMap();
            tags.put(MetricsTag.MODEL.getVal(), modelId);
            final String metricNameSuffix = metricNameSuffix(MetricsCategory.PROJECT.getVal(), project, tags);
            final MetricRegistry registry = MetricsController.getDefaultMetricRegistry();

            removeMetrics(metricNameSuffix, registry);
            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics removeModelMetrics, modelId: {}, projectName: {}, {}", modelId, project,
                    e.getMessage());
        }
        return false;
    }

    private static void removeMetrics(String metricNameSuffix, MetricRegistry registry) {
        synchronized (gauges) {
            final Iterator<String> it = gauges.iterator();
            doRemove(metricNameSuffix, it, registry);
        }

        synchronized (counters) {
            final Iterator<String> it = counters.keySet().iterator();
            doRemove(metricNameSuffix, it, registry);
        }

        synchronized (meters) {
            final Iterator<String> it = meters.keySet().iterator();
            doRemove(metricNameSuffix, it, registry);
        }

        synchronized (histograms) {
            final Iterator<String> it = histograms.keySet().iterator();
            doRemove(metricNameSuffix, it, registry);
        }
    }

    public static Counter getCounter(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name.getVal(), category.getVal(), entity, tags);
        return counters.get(metricName);
    }

    public static <T> Gauge<T> getGauge(MetricsName name, MetricsCategory category, String entity, Map<String, String> tags) {
        final String metricName = metricName(name.getVal(), category.getVal(), entity, tags);
        return MetricsController.getDefaultMetricRegistry().getGauges().get(metricName);
    }

    public static boolean registerProjectMetrics(final String projectName, final String host) {
        try {
            if (StringUtils.isEmpty(projectName)) {
                throw new IllegalArgumentException("registerProjectMetrics, projectName shouldn't be empty.");
            }

            Map<String, String> tags = getHostTagMap(host, projectName);

            // transaction
            newCounter(MetricsName.TRANSACTION_RETRY_COUNTER, MetricsCategory.PROJECT, projectName, tags);
            newHistogram(MetricsName.TRANSACTION_LATENCY, MetricsCategory.PROJECT, projectName, tags);
            // query
            newCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
            Counter denominator = getCounter(MetricsName.QUERY, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, projectName, tags);
            Counter numerator = getCounter(MetricsName.QUERY_LT_1S, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_LT_1S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_1S_3S, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_1S_3S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_3S_5S, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_3S_5S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_5S_10S, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_5S_10S_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_SLOW, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_SLOW_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_FAILED, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_PUSH_DOWN, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_PUSH_DOWN_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_CACHE, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_CACHE_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_AGG_INDEX, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_AGG_INDEX_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, projectName, tags);
            numerator = getCounter(MetricsName.QUERY_TABLE_INDEX, MetricsCategory.PROJECT, projectName, tags);
            newGauge(MetricsName.QUERY_TABLE_INDEX_RATIO, MetricsCategory.PROJECT, projectName, tags,
                    new QueryRatioGauge(numerator, denominator));
            newCounter(MetricsName.QUERY_TOTAL_DURATION, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.QUERY_TIMEOUT, MetricsCategory.PROJECT, projectName, tags);
            newMeter(MetricsName.QUERY_SLOW_RATE, MetricsCategory.PROJECT, projectName, tags);
            newMeter(MetricsName.QUERY_FAILED_RATE, MetricsCategory.PROJECT, projectName, tags);
            newMeter(MetricsName.QUERY_PUSH_DOWN_RATE, MetricsCategory.PROJECT, projectName, tags);
            newMeter(MetricsName.QUERY_TIMEOUT_RATE, MetricsCategory.PROJECT, projectName, tags);
            newHistogram(MetricsName.QUERY_LATENCY, MetricsCategory.PROJECT, projectName, tags);
            // job
            newCounter(MetricsName.JOB, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_DURATION, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_FINISHED, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_STEP_ATTEMPTED, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_FAILED_STEP_ATTEMPTED, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_RESUMED, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_DISCARDED, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_ERROR, MetricsCategory.PROJECT, projectName, tags);
            newHistogram(MetricsName.JOB_DURATION_HISTOGRAM, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.JOB_WAIT_DURATION, MetricsCategory.PROJECT, projectName, tags);
            // metadata management
            newCounter(MetricsName.METADATA_CLEAN, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.METADATA_BACKUP, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.PROJECT, projectName, tags);
            newCounter(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.PROJECT, projectName, tags);

            newHistogram(MetricsName.QUERY_SCAN_BYTES, MetricsCategory.PROJECT, projectName, tags);

            return true;
        } catch (Exception e) {
            logger.warn("ke.metrics registerProjectMetrics, projectName: {} {}", projectName, e.getMessage());
        }
        return false;
    }

    public static void newMetricSet(MetricsName name, MetricsCategory category, String entity, MetricSet metricSet) {
        newMetrics(name.getVal(), metricSet, category, entity);
    }

    private static void newMetrics(String name, MetricSet metricSet, MetricsCategory category, String entity) {
        for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
            Metric value = entry.getValue();
            if (value instanceof MetricSet) {
                newMetrics(name(name, entry.getKey()), (MetricSet) value, category, entity);
            } else {
                newGauge(name(name, entry.getKey()), category, entity, Collections.emptyMap(), value);
            }
        }
    }

    private static String name(String prefix, String part) {
        return "".concat(prefix).concat(".").concat(part);
    }

    public static <T> boolean newGauge(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags, Gauge<T> metric) {
        return newGauge(name.getVal(), category, entity, tags, metric);
    }

    public static <T> boolean newGauge(MetricsName name, MetricsCategory category, String entity, Gauge<T> metric) {
        return newGauge(name.getVal(), category, entity, Collections.emptyMap(), metric);
    }

    private static boolean newGauge(String name, MetricsCategory category, String entity, Map<String, String> tags,
            Metric metric) {
        try {
            return registerGaugeIfAbsent(name, category, entity, tags, metric);
        } catch (Exception e) {
            logger.warn("ke.metrics newGauge {}", e.getMessage());
        }
        return false;
    }

    public static boolean newCounter(MetricsName name, MetricsCategory category, String entity) {
        return newCounter(name, category, entity, Collections.emptyMap());
    }

    public static boolean newCounter(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Counter counter = registerCounterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (counter != null) {
                return true;
            }
        } catch (Exception e) {
            logger.warn("ke.metrics newCounter {}", e.getMessage());
        }
        return false;
    }

    public static boolean newHistogram(MetricsName name, MetricsCategory category, String entity) {
        return newHistogram(name, category, entity, Collections.emptyMap());
    }

    public static boolean newHistogram(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Histogram histogram = registerHistogramIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (histogram != null) {
                return true;
            }
        } catch (Exception e) {
            logger.warn("ke.metrics newHistogram {}", e.getMessage());
        }
        return false;
    }

    public static boolean newMeter(MetricsName name, MetricsCategory category, String entity) {
        return newMeter(name, category, entity, Collections.emptyMap());
    }

    private static boolean newMeter(MetricsName name, MetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Meter meter = registerMeterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (meter != null) {
                return true;
            }
        } catch (Exception e) {
            logger.warn("ke.metrics newMeter {}", e.getMessage());
        }
        return false;
    }

    private static SortedMap<String, String> filterTags(Map<String, String> tags) {
        return new TreeMap<>(tags.entrySet().stream().filter(e -> !"category".equals(e.getKey()))
                .filter(e -> !"entity".equals(e.getKey())).filter(e -> StringUtils.isNotEmpty(e.getValue()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static String metricName(String name, String category, String entity, Map<String, String> tags) {
        Preconditions.checkNotNull(name);
        StringBuilder sb = new StringBuilder(name);
        sb.append(":").append(metricNameSuffix(category, entity, tags));
        return sb.toString();
    }

    private static String metricNameSuffix(String category, String entity, Map<String, String> tags) {
        StringBuilder sb = new StringBuilder();
        sb.append("category=");
        sb.append(category);
        sb.append(",entity=");
        sb.append(entity);

        if (!MapUtils.isEmpty(tags)) {
            final SortedMap<String, String> filteredTags = filterTags(tags);
            if (!filteredTags.isEmpty()) {
                sb.append(",").append(filteredTags.entrySet().stream()
                        .map(e -> String.join("=", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
            }
        }
        return sb.toString();
    }

    private static boolean registerGaugeIfAbsent(String name, MetricsCategory category, String entity,
            Map<String, String> tags, Metric metric) {
        final String metricName = metricName(name, category.getVal(), entity, tags);
        if (!gauges.contains(metricName)) {
            synchronized (gauges) {
                if (!gauges.contains(metricName)) {
                    MetricsController.getDefaultMetricRegistry().register(metricName, metric);
                    gauges.add(metricName);
                    logger.trace("ke.metrics register gauge: {}", metricName);
                    return true;
                }
            }
        }

        return false;
    }

    private static Counter registerCounterIfAbsent(String name, String category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!counters.containsKey(metricName)) {
            synchronized (counters) {
                if (!counters.containsKey(metricName)) {
                    // bad design: 1. Consider async realization; 2. Deadlock maybe occurs here; 3. Add timeout mechanism.
                    final Counter metric = MetricsController.getDefaultMetricRegistry().counter(metricName);
                    final long restoreVal = tryRestoreCounter(name, category, entity, tags);
                    if (restoreVal > 0) {
                        metric.inc(restoreVal);
                        logger.trace("ke.metrics counter=[{}] restore with value: {}", metricName, restoreVal);
                    }
                    counters.put(metricName, metric);
                    logger.trace("ke.metrics register counter: {}", metricName);
                }
            }
        }
        return counters.get(metricName);
    }

    private static long tryRestoreCounter(String fieldName, String category, String entity, Map<String, String> tags) {
        try {
            if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
                return 0;
            }
            final InfluxDB defaultInfluxDb = MetricsInfluxdbReporter.getInstance().getMetricInstance().getInfluxDB();
            if (!defaultInfluxDb.ping().isGood()) {
                throw new IllegalStateException("the pinged influxdb is not good.");
            }

            final KapConfig config = KapConfig.getInstanceFromEnv();
            final StringBuilder sb = new StringBuilder("select ");
            sb.append(fieldName);
            sb.append(" from ");
            sb.append(MetricsInfluxdbReporter.METRICS_MEASUREMENT);
            sb.append(" where category='");
            sb.append(category);
            sb.append("' and entity='");
            sb.append(entity);
            sb.append("'");

            if (!MapUtils.isEmpty(tags)) {
                filterTags(tags).forEach((k, v) -> {
                    sb.append(" and ");
                    sb.append(k);
                    sb.append("='");
                    sb.append(v);
                    sb.append("'");
                });
            }

            sb.append(" order by time desc limit 1;");

            final String querySql = sb.toString();

            final QueryResult result = defaultInfluxDb
                    .query(new Query(querySql, config.getMetricsDbNameWithMetadataUrlPrefix()));
            if (CollectionUtils.isEmpty(result.getResults().get(0).getSeries())) {
                logger.trace("ke.metrics tryRestoreCounter, got empty series, sql=[{}]", querySql);
                return 0;
            }
            QueryResult.Series series = result.getResults().get(0).getSeries().get(0);
            String valStr = fieldName.equals(series.getColumns().get(1))
                    ? String.valueOf(series.getValues().get(0).get(1))
                    : String.valueOf(series.getValues().get(0).get(0));

            logger.trace("ke.metrics tryRestoreCounter, sql=[{}], result=[{}]", querySql, valStr);
            return NumberFormat.getInstance(Locale.getDefault(Locale.Category.FORMAT)).parse(valStr).longValue();
        } catch (Exception e) {
            logger.warn(
                    "ke.metrics tryRestoreCounter error. fieldName: [{}], category [{}], entity [{}], tags [{}]. error msg {}",
                    fieldName, category, entity, tags, e.getMessage());
        }
        return 0;
    }

    private static Meter registerMeterIfAbsent(String name, String category, String entity, Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!meters.containsKey(metricName)) {
            synchronized (meters) {
                if (!meters.containsKey(metricName)) {
                    final Meter metric = MetricsController.getDefaultMetricRegistry().meter(metricName);
                    meters.put(metricName, metric);
                    logger.trace("ke.metrics register meter: {}", metricName);
                }
            }
        }
        return meters.get(metricName);
    }

    private static Histogram registerHistogramIfAbsent(String name, String category, String entity,
            Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!histograms.containsKey(metricName)) {
            synchronized (histograms) {
                if (!histograms.containsKey(metricName)) {
                    final Histogram metric = MetricsController.getDefaultMetricRegistry().histogram(metricName);
                    histograms.put(metricName, metric);
                    logger.trace("ke.metrics register histogram: {}", metricName);
                }
            }
        }
        return histograms.get(metricName);
    }

    private static void doRemove(final String metricNameSuffix, final Iterator<String> it,
            final MetricRegistry registry) {
        // replace with removeIf
        while (it.hasNext()) {
            //some1:k1=v1,k2=v2,k3=v3,...
            final String metricName = it.next();
            try {
                String[] arr = metricName.split(":", 2);
                if (metricNameSuffix.equals(arr[1]) || arr[1].startsWith(metricNameSuffix + ",")) {
                    registry.remove(metricName);
                    it.remove();
                    logger.trace("ke.metrics remove metric: {}", metricName);
                }
            } catch (Exception e) {
                logger.warn("ke.metrics remove metric: {} {}", metricName, e.getMessage());
            }
        }
    }

    public static Map<String, String> getHostTagMap(String entity) {
        String host = AddressUtil.getZkLocalInstance();
        return getHostTagMap(host, entity);
    }

    public static Map<String, String> getHostTagMap(String host, String entity) {
        StringBuilder sb = new StringBuilder(host);
        sb.append("-").append(entity);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), sb.toString());
        return tags;
    }
}
