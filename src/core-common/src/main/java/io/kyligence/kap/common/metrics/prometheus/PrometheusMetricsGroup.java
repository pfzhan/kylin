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

package io.kyligence.kap.common.metrics.prometheus;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.codahale.metrics.Counter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.util.AddressUtil;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.lang.Nullable;
import lombok.val;

@Component
public class PrometheusMetricsGroup {
    private static final Logger logger = LoggerFactory.getLogger(PrometheusMetricsGroup.class);

    public static final String TAG_NAME_KYLIN_SERVER = "kylin_server";
    public static final String TAG_NAME_PROJECT = "project";
    public static final String TAG_NAME_MODEL = "model_name";

    private static MeterRegistry meterRegistry;

    @Autowired
    @Lazy
    public PrometheusMetricsGroup(MeterRegistry tempMeterRegistry) {
        meterRegistry = tempMeterRegistry;
    }

    public static void removeProjectMetrics(String project) {
        if (StringUtils.isEmpty(project)) {
            throw new IllegalArgumentException("Remove prometheus project metrics, project shouldn't be empty.");
        }

        meterRegistry.getMeters().stream().map(Meter::getId).filter(id -> project.equals(id.getTag(TAG_NAME_PROJECT)))
                .forEach(id -> meterRegistry.remove(id));

        logger.info("Remove project prometheus metrics for {} success.", project);
    }

    public static void removeModelMetrics(String project, String modelName) {
        if (StringUtils.isBlank(project) || StringUtils.isBlank(modelName)) {
            throw new IllegalArgumentException(
                    "Remove prometheus model metrics, project or modelName shouldn't be empty.");
        }

        Set<PrometheusMetrics> modelMetrics = PrometheusMetrics.listModelMetrics();
        Tags tags = generateModelTags(project, modelName);

        modelMetrics.forEach(metricName -> doRemoveMetric(metricName, tags));
    }

    private static void doRemoveMetric(PrometheusMetrics metricName, Tags tags) {
        Meter.Id id = generateMeterId(metricName, tags, Meter.Type.GAUGE);
        Meter result = meterRegistry.remove(id);
        if (Objects.isNull(result)) {
            logger.warn("Remove prometheus metric failed, metric name: {}, tags: {}", metricName.getValue(), tags);
        }
    }

    public static void newMetrics() {
        Tags tags = generateInstanceTags();

        List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        newGaugeIfAbsent(PrometheusMetrics.JVM_GC_PAUSE_TIME, garbageCollectorMXBeans,
                v -> v.stream().mapToDouble(GarbageCollectorMXBean::getCollectionTime).sum(), tags);

        for (String state : Lists.newArrayList("idle", "active")) {
            JdbcDataSource.getDataSources().stream()
                    .collect(Collectors.groupingBy(ds -> ((BasicDataSource) ds).getDriverClassName()))
                    .forEach((driver, sources) -> {
                        newGaugeIfAbsent(PrometheusMetrics.JVM_DB_CONNECTIONS, sources, dataSources -> {
                            int count = 0;
                            for (DataSource dataSource : dataSources) {
                                BasicDataSource basicDataSource = (BasicDataSource) dataSource;
                                if (state.equals("idle")) {
                                    count += basicDataSource.getNumIdle();
                                } else {
                                    count += basicDataSource.getNumActive();
                                }
                            }
                            return count;
                        }, Tags.of("state", state, "pool", "dbcp2", "type", driver, "instance",
                                AddressUtil.getZkLocalInstance()));
                    });
        }

    }

    public static void newCounterFromDropwizard(PrometheusMetrics metric, String project,
            Map<String, String> dropwizardTags, Tags tags) {
        newCounterFromDropwizard(metric, metric.toMetricsName(), project, dropwizardTags, tags);
    }

    public static void newCounterFromDropwizard(PrometheusMetrics metric, MetricsName dropwizardMetric, String project,
            Map<String, String> dropwizardTags, Tags tags) {
        com.codahale.metrics.Counter counter = MetricsGroup.getCounter(dropwizardMetric, MetricsCategory.PROJECT,
                project, dropwizardTags);
        if (Objects.nonNull(counter)) {
            newGaugeIfAbsent(metric, counter, Counter::getCount, tags);
        }
    }

    public static void newGaugeFromDropwizard(PrometheusMetrics metric, String project,
            Map<String, String> dropwizardTags, Tags tags) {
        newGaugeFromDropwizard(metric, metric.toMetricsName(), project, dropwizardTags, tags);
    }

    public static void newGaugeFromDropwizard(PrometheusMetrics metric, MetricsName dropwizardMetric, String project,
            Map<String, String> dropwizardTags, Tags tags) {
        com.codahale.metrics.Gauge<Long> gauge = MetricsGroup.getGauge(dropwizardMetric, MetricsCategory.PROJECT,
                project, dropwizardTags);
        if (Objects.nonNull(gauge)) {
            newGaugeIfAbsent(metric, gauge, com.codahale.metrics.Gauge::getValue, tags);
        }
    }

    public static <T> void newProjectGauge(PrometheusMetrics metric, String project, @Nullable T obj,
            ToDoubleFunction<T> function) {
        Tags prometheusTags = generateProjectTags(project);
        newGaugeIfAbsent(metric, obj, function, prometheusTags);
    }

    public static <T> void newProjectGaugeWithoutServerTag(PrometheusMetrics metric, String project, @Nullable T obj,
            ToDoubleFunction<T> function) {
        Tag projectTag = Tag.of(TAG_NAME_PROJECT, project);
        Tags tags = Tags.of(projectTag);
        newGaugeIfAbsent(metric, obj, function, tags);
    }

    public static <T> void newModelGauge(PrometheusMetrics metric, String project, String model, @Nullable T obj,
            ToDoubleFunction<T> function) {
        Tags prometheusTags = generateModelTags(project, model);
        newGaugeIfAbsent(metric, obj, function, prometheusTags);
    }

    public static <T> void newGaugeIfAbsent(PrometheusMetrics metric, @Nullable T obj, ToDoubleFunction<T> function,
            Tags tags) {
        Meter.Id meterId = generateMeterId(metric, tags, Meter.Type.GAUGE);
        boolean exists = meterRegistry.getMeters().stream().map(Meter::getId).anyMatch(id -> id.equals(meterId));
        if (!exists) {
            Gauge.builder(metric.getValue(), obj, function).strongReference(true).tags(tags).register(meterRegistry);
            logger.trace("Create a new gauge, metric name: {}, tags: {}", metric.getValue(), tags);
        }
    }

    public static void summaryRecord(double amount, PrometheusMetrics metric, double[] percentiles, String... tags) {
        val builder = DistributionSummary.builder(metric.getValue()).tags(tags)
                .distributionStatisticExpiry(Duration.ofDays(1));
        if (ArrayUtils.isNotEmpty(percentiles)) {
            builder.publishPercentileHistogram(true).publishPercentiles(percentiles);
        }
        builder.register(meterRegistry).record(amount);
    }

    public static Tags generateInstanceTags() {
        Tag kylinServer = Tag.of(TAG_NAME_KYLIN_SERVER, AddressUtil.getZkLocalInstance());
        return Tags.of(kylinServer);
    }

    public static Tags generateProjectTags(String project) {
        Tag kylinServer = Tag.of(TAG_NAME_KYLIN_SERVER, AddressUtil.getZkLocalInstance());
        Tag projectTag = Tag.of(TAG_NAME_PROJECT, project);

        return Tags.of(Sets.newHashSet(kylinServer, projectTag));
    }

    public static Tags generateModelTags(String project, String modelName) {
        Tag modelTag = Tag.of(TAG_NAME_MODEL, modelName);
        return generateProjectTags(project).and(modelTag);
    }

    private static Meter.Id generateMeterId(PrometheusMetrics metric, Tags tags, Meter.Type type) {
        return new Meter.Id(metric.getValue(), tags, null, null, type);
    }

}
