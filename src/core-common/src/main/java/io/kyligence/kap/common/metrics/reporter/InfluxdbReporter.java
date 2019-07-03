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

package io.kyligence.kap.common.metrics.reporter;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point;

public class InfluxdbReporter extends ScheduledReporter {

    private static final Logger logger = LoggerFactory.getLogger(InfluxdbReporter.class);

    private final InfluxDB influxDb;

    private final Clock clock;

    private final Transformer transformer;

    private final String defaultMeasurement;

    private static final String COUNT = "count";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String MEAN = "mean";
    private static final String STANDARD_DEVIATION = "std-dev";
    private static final String FIFTY_PERCENTILE = "50p";
    private static final String SEVENTY_FIVE_PERCENTILE = "75p";
    private static final String NINETY_FIVE_PERCENTILE = "95p";
    private static final String NINETY_NINE_PERCENTILE = "99p";
    private static final String NINETY_NINE_POINT_NINE_PERCENTILE = "999p";
    private static final String RUN_COUNT = "run-count";
    private static final String ONE_MINUTE = "1-minute";
    private static final String FIVE_MINUTE = "5-minute";
    private static final String FIFTEEN_MINUTE = "15-minute";
    private static final String MEAN_MINUTE = "mean-minute";


    public InfluxdbReporter(InfluxDB influxDb, String defaultMeasurement, MetricRegistry registry, String name) {
        super(registry, name, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        this.influxDb = influxDb;
        this.clock = Clock.defaultClock();
        this.transformer = new Transformer();
        this.defaultMeasurement = defaultMeasurement;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

        try {
            long startAt = System.currentTimeMillis();
            if (!influxDb.ping().isGood()) {
                throw new IllegalStateException("the pinged influxdb is not good.");
            }

            final long timestamp = clock.getTime();

            final ImmutableList<Point> points = ImmutableList.<Point> builder()
                    .addAll(transformer.fromGauges(gauges, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromCounters(counters, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromHistograms(histograms, defaultMeasurement, timestamp,
                            TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromMeters(meters, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .addAll(transformer.fromTimers(timers, defaultMeasurement, timestamp, TimeUnit.MILLISECONDS))
                    .build();

            points.forEach(p -> influxDb.write(p));
            influxDb.flush();

            NMetricsGroup.counterInc(NMetricsName.SUMMARY_COUNTER, NMetricsCategory.GLOBAL, "global");
            NMetricsGroup.counterInc(NMetricsName.SUMMARY_DURATION, NMetricsCategory.GLOBAL, "global",
                    System.currentTimeMillis() - startAt);

            logger.debug("kap.metrics report data: {} points", points.size());
        } catch (Exception e) {
            logger.warn("kap.metrics report data failed", e);
        }
    }

    private class Transformer {

        public List<Point> fromGauges(final Map<String, Gauge> gauges, final String measurement, final long timestamp,
                final TimeUnit timeUnit) {
            return fromGaugesOrCounters(gauges, Gauge::getValue, measurement, timestamp, timeUnit);
        }

        public List<Point> fromCounters(final Map<String, Counter> counters, final String measurement,
                final long timestamp, final TimeUnit timeUnit) {
            return fromGaugesOrCounters(counters, Counter::getCount, measurement, timestamp, timeUnit);
        }

        public List<Point> fromHistograms(final Map<String, Histogram> histograms, final String measurement,
                final long timestamp, final TimeUnit timeUnit) {

            return histograms.entrySet().stream().map(e -> {
                Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                final Histogram histogram = e.getValue();
                final Snapshot snapshot = histogram.getSnapshot();

                return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                        .putField(filedName(nameTags.getFirst(), COUNT), snapshot.size())
                        .putField(filedName(nameTags.getFirst(), MIN), snapshot.getMin())
                        .putField(filedName(nameTags.getFirst(), MAX), snapshot.getMax())
                        .putField(filedName(nameTags.getFirst(), MEAN), snapshot.getMean())
                        .putField(filedName(nameTags.getFirst(), STANDARD_DEVIATION), snapshot.getStdDev())
                        .putField(filedName(nameTags.getFirst(), FIFTY_PERCENTILE), snapshot.getMedian())
                        .putField(filedName(nameTags.getFirst(), SEVENTY_FIVE_PERCENTILE), snapshot.get75thPercentile())
                        .putField(filedName(nameTags.getFirst(), NINETY_FIVE_PERCENTILE), snapshot.get95thPercentile())
                        .putField(filedName(nameTags.getFirst(), NINETY_NINE_PERCENTILE), snapshot.get99thPercentile())
                        .putField(filedName(nameTags.getFirst(), NINETY_NINE_POINT_NINE_PERCENTILE), snapshot.get999thPercentile())
                        .putField(filedName(nameTags.getFirst(), RUN_COUNT), histogram.getCount()).build();
            }).collect(toList());
        }

        public List<Point> fromMeters(final Map<String, Meter> meters, final String measurement, final long timestamp,
                final TimeUnit timeUnit) {
            return meters.entrySet().stream().map(e -> {
                Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                final Meter meter = e.getValue();

                return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                        .putField(filedName(nameTags.getFirst(), COUNT), meter.getCount())
                        .putField(filedName(nameTags.getFirst(), ONE_MINUTE), convertRate(meter.getOneMinuteRate()))
                        .putField(filedName(nameTags.getFirst(), FIVE_MINUTE), convertRate(meter.getFiveMinuteRate()))
                        .putField(filedName(nameTags.getFirst(), FIFTEEN_MINUTE),
                                convertRate(meter.getFifteenMinuteRate()))
                        .putField(filedName(nameTags.getFirst(), MEAN_MINUTE), convertRate(meter.getMeanRate()))
                        .build();
            }).collect(toList());
        }

        public List<Point> fromTimers(final Map<String, Timer> timers, final String measurement, final long timestamp,
                final TimeUnit timeUnit) {
            return timers.entrySet().stream().map(e -> {
                Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                final Timer timer = e.getValue();
                final Snapshot snapshot = timer.getSnapshot();

                return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                        .putField(filedName(nameTags.getFirst(), COUNT), snapshot.size())
                        .putField(filedName(nameTags.getFirst(), MIN), convertDuration(snapshot.getMin()))
                        .putField(filedName(nameTags.getFirst(), MAX), convertDuration(snapshot.getMax()))
                        .putField(filedName(nameTags.getFirst(), MEAN), convertDuration(snapshot.getMean()))
                        .putField(filedName(nameTags.getFirst(), STANDARD_DEVIATION), convertDuration(snapshot.getStdDev()))
                        .putField(filedName(nameTags.getFirst(), FIFTY_PERCENTILE), convertDuration(snapshot.getMedian()))
                        .putField(filedName(nameTags.getFirst(), SEVENTY_FIVE_PERCENTILE), convertDuration(snapshot.get75thPercentile()))
                        .putField(filedName(nameTags.getFirst(), NINETY_FIVE_PERCENTILE), convertDuration(snapshot.get95thPercentile()))
                        .putField(filedName(nameTags.getFirst(), NINETY_NINE_PERCENTILE), convertDuration(snapshot.get99thPercentile()))
                        .putField(filedName(nameTags.getFirst(), NINETY_NINE_POINT_NINE_PERCENTILE),
                                convertDuration(snapshot.get999thPercentile()))
                        .putField(filedName(nameTags.getFirst(), ONE_MINUTE), convertRate(timer.getOneMinuteRate()))
                        .putField(filedName(nameTags.getFirst(), FIVE_MINUTE), convertRate(timer.getFiveMinuteRate()))
                        .putField(filedName(nameTags.getFirst(), FIFTEEN_MINUTE),
                                convertRate(timer.getFifteenMinuteRate()))
                        .putField(filedName(nameTags.getFirst(), MEAN_MINUTE), convertRate(timer.getMeanRate()))
                        .putField(filedName(nameTags.getFirst(), RUN_COUNT), timer.getCount()).build();
            }).collect(toList());
        }

        private <T, R> List<Point> fromGaugesOrCounters(final Map<String, T> items, final Function<T, R> valueExtractor,
                final String measurement, final long timestamp, final TimeUnit timeUnit) {
            return items.entrySet().stream().map(e -> {
                Pair<String, Map<String, String>> nameTags = parseNameTags(e.getKey());
                final R value = valueExtractor.apply(e.getValue());
                return new PointBuilder(measurement, timestamp, timeUnit).putTags(nameTags.getSecond())
                        .putField(nameTags.getFirst(), value).build();
            }).collect(toList());
        }

        private Pair<String, Map<String, String>> parseNameTags(String metricName) {

            Preconditions.checkNotNull(metricName);

            String[] nameTags = metricName.split(":", 2);
            Map<String, String> tags = Arrays.asList(nameTags[1].split(",")).stream().map(t -> {
                String[] keyVal = t.split("=", 2);
                return new AbstractMap.SimpleEntry<>(keyVal[0], keyVal[1]);
            }).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            return Pair.newPair(nameTags[0], tags);
        }

        private String filedName(String prefix, String suffix) {
            return String.join("_", prefix, suffix);
        }
    }

    private static final Set<Class> VALID_FIELD_CLASSES = ImmutableSet.of(Boolean.class, Byte.class, Character.class,
            Double.class, Float.class, Integer.class, Long.class, Short.class, String.class);

    private class PointBuilder {

        // I definitely wanna a Measurement.Builder here, 
        // but it couldn't be realized in a inner class.
        private final String measurement;
        private final long timestamp;
        private final TimeUnit timeUnit;
        private final Map<String, String> tags = new TreeMap<>();
        private final Map<String, Object> fields = new TreeMap<>();

        public PointBuilder(String measurement, long timestamp, TimeUnit timeUnit) {
            this.measurement = measurement;
            this.timestamp = timestamp;
            this.timeUnit = timeUnit;
        }

        private String handleCollection(final String key, final Collection collection) {
            for (final Object value : collection) {
                if (!isValidField(value)) {
                    throw new IllegalArgumentException(String.format(
                            "Measure collection field '%s' must contain only Strings and primitives: invalid field '%s'",
                            key, value));
                }
            }
            return collection.toString();
        }

        private <T> boolean isValidField(final T value) {
            return value == null || VALID_FIELD_CLASSES.contains(value.getClass());
        }

        private <T> Optional<T> handleField(final String key, final T value) {
            if (value instanceof Float) {
                final float f = (Float) value;
                if (!Float.isNaN(f) && !Float.isInfinite(f)) {
                    return Optional.of(value);
                }
            } else if (value instanceof Double) {
                final double d = (Double) value;
                if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                    return Optional.of(value);
                }
            } else if (value instanceof Number) {
                return Optional.of(value);
            } else if (value instanceof String || value instanceof Character || value instanceof Boolean) {
                return Optional.of(value);
            } /*else {
                //TODO handle exception
                throw new IllegalArgumentException(String.format(
                        "Measure field '%s' must be a String, primitive, or Collection: invalid field '%s'", key,
                        value));
              }*/

            return Optional.empty();
        }

        public PointBuilder putTag(final String key, final String value) {
            tags.put(key, value);
            return this;
        }

        public PointBuilder putTags(final Map<String, String> items) {
            tags.putAll(items);
            return this;
        }

        public <T> PointBuilder putField(final String key, final T value) {

            if (value instanceof Collection<?>) {
                fields.put(key, handleCollection(key, (Collection) value));
            } else if (value != null) {
                handleField(key, value).ifPresent(s -> fields.put(key, s));
            }

            return this;
        }

        public <T> PointBuilder putFields(final Map<String, T> items) {
            items.forEach(this::putField);
            return this;
        }

        public Point build() {
            return Point.measurement(measurement).time(timestamp, timeUnit).tag(tags).fields(fields).build();
        }
    }
}
