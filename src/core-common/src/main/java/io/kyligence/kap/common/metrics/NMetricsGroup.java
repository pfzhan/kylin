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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;

public class NMetricsGroup {

    //TODO redesign: event loop, metrics registering shouldn't block the main process 

    private static final Logger logger = LoggerFactory.getLogger(NMetricsGroup.class);

    private static final Set<String> gauges = Collections.synchronizedSet(new HashSet<>());

    private static final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap();

    private static final ConcurrentHashMap<String, Meter> meters = new ConcurrentHashMap();

    private static final ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap();

    public static <T> boolean newGauge(NMetricsName name, NMetricsCategory category, String entity, Gauge<T> metric) {
        return newGauge(name, category, entity, Collections.emptyMap(), metric);
    }

    public static boolean newCounter(NMetricsName name, NMetricsCategory category, String entity) {
        return newCounter(name, category, entity, Collections.emptyMap());
    }

    public static boolean newHistogram(NMetricsName name, NMetricsCategory category, String entity) {
        return newHistogram(name, category, entity, Collections.emptyMap());
    }

    public static boolean newMeter(NMetricsName name, NMetricsCategory category, String entity) {
        return newMeter(name, category, entity, Collections.emptyMap());
    }

    public static boolean counterInc(NMetricsName name, NMetricsCategory category, String entity) {
        return counterInc(name, category, entity, Collections.emptyMap());
    }

    public static boolean counterInc(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Counter counter = registerCounterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (counter != null) {
                counter.inc();
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics counterInc", e);
        }
        return false;
    }

    public static boolean counterInc(NMetricsName name, NMetricsCategory category, String entity, long increments) {
        try {
            final Counter counter = registerCounterIfAbsent(name.getVal(), category.getVal(), entity,
                    Collections.emptyMap());
            if (counter != null) {
                counter.inc(increments);
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics counterInc", e);
        }
        return false;
    }

    public static boolean histogramUpdate(NMetricsName name, NMetricsCategory category, String entity, long updateTo) {
        return histogramUpdate(name, category, entity, Collections.emptyMap(), updateTo);
    }

    public static boolean histogramUpdate(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags, long updateTo) {
        try {
            final Histogram histogram = registerHistogramIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (histogram != null) {
                histogram.update(updateTo);
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics histogramUpdate", e);
        }
        return false;
    }

    public static boolean meterMark(NMetricsName name, NMetricsCategory category, String entity) {
        return meterMark(name, category, entity, Collections.emptyMap());
    }

    public static boolean meterMark(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Meter meter = registerMeterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (meter != null) {
                meter.mark();
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics meterMark", e);
        }
        return false;
    }

    public static boolean removeProjectMetrics(final String projectName) {
        try {
            if (StringUtils.isEmpty(projectName)) {
                throw new IllegalArgumentException("removeProjectMetrics, projectName shouldn't be empty.");
            }
            StringBuilder sb = new StringBuilder("category=");
            sb.append(NMetricsCategory.PROJECT.getVal());
            sb.append(",entity=");
            sb.append(projectName);
            final String projectTag = sb.toString();
            final MetricRegistry registry = NMetricsController.getDefaultMetricRegistry();

            synchronized (gauges) {
                final Iterator<String> it = gauges.iterator();
                doRemove(projectTag, it, registry);
            }

            synchronized (counters) {
                final Iterator<String> it = counters.keySet().iterator();
                doRemove(projectTag, it, registry);
            }

            synchronized (meters) {
                final Iterator<String> it = meters.keySet().iterator();
                doRemove(projectTag, it, registry);
            }

            synchronized (histograms) {
                final Iterator<String> it = histograms.keySet().iterator();
                doRemove(projectTag, it, registry);
            }
            return true;
        } catch (Exception e) {
            logger.error("kap.metrics removeProjectMetrics, projectName: {}", projectName, e);
        }
        return false;
    }

    public static boolean registerProjectMetrics(final String projectName) {
        try {
            if (StringUtils.isEmpty(projectName)) {
                throw new IllegalArgumentException("registerProjectMetrics, projectName shouldn't be empty.");
            }
            // transaction
            newCounter(NMetricsName.TRANSACTION_RETRY_COUNTER, NMetricsCategory.PROJECT, projectName);
            newHistogram(NMetricsName.TRANSACTION_LATENCY, NMetricsCategory.PROJECT, projectName);
            // query
            newCounter(NMetricsName.QUERY, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.QUERY_SLOW, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.QUERY_FAILED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.QUERY_PUSH_DOWN, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.QUERY_TIMEOUT, NMetricsCategory.PROJECT, projectName);
            newMeter(NMetricsName.QUERY_SLOW_RATE, NMetricsCategory.PROJECT, projectName);
            newMeter(NMetricsName.QUERY_FAILED_RATE, NMetricsCategory.PROJECT, projectName);
            newMeter(NMetricsName.QUERY_PUSH_DOWN_RATE, NMetricsCategory.PROJECT, projectName);
            newMeter(NMetricsName.QUERY_TIMEOUT_RATE, NMetricsCategory.PROJECT, projectName);
            newHistogram(NMetricsName.QUERY_LATENCY, NMetricsCategory.PROJECT, projectName);
            // job
            newCounter(NMetricsName.JOB, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_DURATION, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_FINISHED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_STEP_ATTEMPTED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_FAILED_STEP_ATTEMPTED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_RESUMED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_DISCARDED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.JOB_ERROR, NMetricsCategory.PROJECT, projectName);
            newHistogram(NMetricsName.JOB_DURATION_HISTOGRAM, NMetricsCategory.PROJECT, projectName);
            // metadata management
            newCounter(NMetricsName.METADATA_CLEAN, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.METADATA_BACKUP, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.METADATA_BACKUP_DURATION, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.PROJECT, projectName);
            // favorite queue
            newCounter(NMetricsName.FQ_FE_INVOKED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_BE_INVOKED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_BE_INVOKED_DURATION, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_BE_INVOKED_FAILED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_ADJUST_INVOKED, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_ADJUST_INVOKED_DURATION, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_UPDATE_USAGE, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_UPDATE_USAGE_DURATION, NMetricsCategory.PROJECT, projectName);
            newCounter(NMetricsName.FQ_FAILED_UPDATE_USAGE, NMetricsCategory.PROJECT, projectName);
            // event statistics
            newCounter(NMetricsName.EVENT_COUNTER, NMetricsCategory.PROJECT, projectName);

            return true;
        } catch (Exception e) {
            logger.error("kap.metrics registerProjectMetrics, projectName: {}", projectName, e);
        }
        return false;
    }

    private static <T> boolean newGauge(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags, Gauge<T> metric) {
        try {
            return registerGaugeIfAbsent(name, category, entity, tags, metric);
        } catch (Exception e) {
            logger.error("kap.metrics newGauge", e);
        }
        return false;
    }

    private static boolean newCounter(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Counter counter = registerCounterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (counter != null) {
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics newCounter", e);
        }
        return false;
    }

    private static boolean newHistogram(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Histogram histogram = registerHistogramIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (histogram != null) {
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics newHistogram", e);
        }
        return false;
    }

    private static boolean newMeter(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags) {
        try {
            final Meter meter = registerMeterIfAbsent(name.getVal(), category.getVal(), entity, tags);
            if (meter != null) {
                return true;
            }
        } catch (Exception e) {
            logger.error("kap.metrics newMeter", e);
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
        Preconditions.checkNotNull(category);
        Preconditions.checkNotNull(entity);

        StringBuilder sb = new StringBuilder(name);
        sb.append(":category=");
        sb.append(category);
        sb.append(",entity=");
        sb.append(entity);

        if (!MapUtils.isEmpty(tags)) {
            final SortedMap<String, String> filteredTags = filterTags(tags);
            if (!filteredTags.isEmpty()) {
                sb.append(",").append(String.join(",", filteredTags.entrySet().stream()
                        .map(e -> String.join("=", e.getKey(), e.getValue())).collect(toList())));
            }
        }
        return sb.toString();
    }

    private static <T> boolean registerGaugeIfAbsent(NMetricsName name, NMetricsCategory category, String entity,
            Map<String, String> tags, Gauge<T> metric) {
        final String metricName = metricName(name.getVal(), category.getVal(), entity, tags);
        if (!gauges.contains(metricName)) {
            synchronized (gauges) {
                if (!gauges.contains(metricName)) {
                    NMetricsController.getDefaultMetricRegistry().register(metricName, metric);
                    gauges.add(metricName);
                    logger.info("kap.metrics register gauge: {}", metricName);
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
                    //TODO bad design 
                    // 1. Consider async realization;
                    // 2. Deadlock maybe occurs here;
                    // 3. Add timeout mechanism.
                    final Counter metric = NMetricsController.getDefaultMetricRegistry().counter(metricName);
                    final long restoreVal = tryRestoreCounter(name, category, entity, tags);
                    if (restoreVal > 0) {
                        metric.inc(restoreVal);
                        logger.info("kap.metrics counter=[{}] restore with value: {}", metricName, restoreVal);
                    }
                    counters.put(metricName, metric);
                    logger.info("kap.metrics register counter: {}", metricName);
                }
            }
        }
        return counters.get(metricName);
    }

    private static long tryRestoreCounter(String fieldName, String category, String entity, Map<String, String> tags) {
        try {
            final InfluxDB defaultInfluxDb = NMetricsController.getDefaultInfluxDb();
            if (!defaultInfluxDb.ping().isGood()) {
                throw new IllegalStateException("the pinged influxdb is not good.");
            }

            final KapConfig config = KapConfig.getInstanceFromEnv();
            final StringBuilder sb = new StringBuilder("select ");
            sb.append(fieldName);
            sb.append(" from ");
            sb.append(config.getMetricsInfluxMeasurement());
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

            final QueryResult result = defaultInfluxDb.query(new Query(querySql, config.getMetricsInfluxDbName()));
            if (CollectionUtils.isEmpty(result.getResults().get(0).getSeries())) {
                logger.info("kap.metrics tryRestoreCounter, got empty series, sql=[{}]", querySql);
                return 0;
            }
            QueryResult.Series series = result.getResults().get(0).getSeries().get(0);
            String valStr = fieldName.equals(series.getColumns().get(1))
                    ? String.valueOf(series.getValues().get(0).get(1))
                    : String.valueOf(series.getValues().get(0).get(0));

            logger.debug("kap.metrics tryRestoreCounter, sql=[{}], result=[{}]", querySql, valStr);
            return NumberFormat.getInstance().parse(valStr).longValue();
        } catch (Exception e) {
            logger.error("kap.metrics tryRestoreCounter error", e);
        }
        return 0;
    }

    private static Meter registerMeterIfAbsent(String name, String category, String entity, Map<String, String> tags) {
        final String metricName = metricName(name, category, entity, tags);
        if (!meters.containsKey(metricName)) {
            synchronized (meters) {
                if (!meters.containsKey(metricName)) {
                    final Meter metric = NMetricsController.getDefaultMetricRegistry().meter(metricName);
                    meters.put(metricName, metric);
                    logger.info("kap.metrics register meter: {}", metricName);
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
                    final Histogram metric = NMetricsController.getDefaultMetricRegistry().histogram(metricName);
                    histograms.put(metricName, metric);
                    logger.info("kap.metrics register histogram: {}", metricName);
                }
            }
        }
        return histograms.get(metricName);
    }

    private static void doRemove(final String projectTag, final Iterator<String> it, final MetricRegistry registry) {
        //TODO replace with removeIf
        while (it.hasNext()) {
            //some1:k1=v1,k2=v2,k3=v3,...
            final String metricName = it.next();
            try {
                String[] arr = metricName.split(":", 2);
                if (projectTag.equals(arr[1]) || arr[1].startsWith(projectTag + ",")) {
                    registry.remove(metricName);
                    it.remove();
                    logger.info("kap.metrics remove metric: {}", metricName);
                }
            } catch (Exception e) {
                logger.error("kap.metrics remove metric: {}", metricName, e);
            }
        }
    }

}
