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

package io.kyligence.kap.common.metric.prometheus;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsNameEnum;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ MetricsGroup.class })
public class PrometheusMetricsGroupTest extends NLocalFileMetadataTestCase {

    private MeterRegistry meterRegistry;

    private String project = "default";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        PrometheusMetricsGroup prometheusMetricsGroup = new PrometheusMetricsGroup(new SimpleMeterRegistry());
        meterRegistry = (MeterRegistry) ReflectionTestUtils.getField(prometheusMetricsGroup,
                "meterRegistry");
        PowerMockito.mockStatic(MetricsGroup.class);
    }

    @Test
    public void testNewJvmGcPauseMetric() {
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertFalse(meters.stream().anyMatch(
                meter -> PrometheusMetricsNameEnum.JVM_GC_PAUSE_TIME.getValue().equals(meter.getId().getName())));

        PrometheusMetricsGroup.newJvmGcPauseMetric();

        meters = meterRegistry.getMeters();
        Assert.assertTrue(meters.stream().anyMatch(
                meter -> PrometheusMetricsNameEnum.JVM_GC_PAUSE_TIME.getValue().equals(meter.getId().getName())));
    }

    @Test
    public void testNewMetricFromDropwizardCounterWithHostTag() {
        PrometheusMetricsGroup.newMetricFromDropwizardCounterWithHostTag(PrometheusMetricsNameEnum.JOB_PENDING_NUM, project);
        List<Meter> meters = meterRegistry.getMeters();

        Assert.assertEquals(0, meters.size());

        Counter counter = Mockito.mock(Counter.class);
        PowerMockito.when(MetricsGroup.getCounter(PrometheusMetricsNameEnum.QUERY_TIMES.toMetricsName(),
                MetricsCategory.PROJECT, project, Collections.emptyMap())).thenReturn(counter);

        PrometheusMetricsGroup.newMetricFromDropwizardCounterWithHostTag(PrometheusMetricsNameEnum.QUERY_TIMES, project);
        meters = meterRegistry.getMeters();
        Assert.assertEquals(1, meters.size());
        Assert.assertTrue(meters.stream().anyMatch(
                meter -> PrometheusMetricsNameEnum.QUERY_TIMES.getValue().equals(meter.getId().getName())));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid metrics name: JVM_GC_PAUSE_TIME");
        PrometheusMetricsGroup.newMetricFromDropwizardCounterWithHostTag(PrometheusMetricsNameEnum.JVM_GC_PAUSE_TIME, project);
    }

    @Test
    public void testNewMetricFromDropwizardGaugeWithoutHostTag() {
        PrometheusMetricsGroup.newMetricFromDropwizardGaugeWithoutHostTag(PrometheusMetricsNameEnum.JOB_PENDING_NUM, project);
        List<Meter> meters = meterRegistry.getMeters();

        Assert.assertEquals(0, meters.size());

        Gauge gauge = Mockito.mock(Gauge.class);
        PowerMockito.when(MetricsGroup.getGauge(PrometheusMetricsNameEnum.JOB_PENDING_NUM.toMetricsName(),
                MetricsCategory.PROJECT, project, Collections.emptyMap())).thenReturn(gauge);

        PrometheusMetricsGroup.newMetricFromDropwizardGaugeWithoutHostTag(PrometheusMetricsNameEnum.JOB_PENDING_NUM, project);
        meters = meterRegistry.getMeters();
        Assert.assertEquals(1, meters.size());
        Assert.assertTrue(meters.stream().anyMatch(
                meter -> PrometheusMetricsNameEnum.JOB_PENDING_NUM.getValue().equals(meter.getId().getName())));
    }

    @Test
    public void testNewProjectGauge() {
        AtomicLong ref = new AtomicLong(0);
        PrometheusMetricsGroup.newProjectGauge(PrometheusMetricsNameEnum.JOB_WAIT_DURATION_MAX, project, ref, AtomicLong::get);
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertEquals(1, meters.size());
    }

    @Test
    public void testNewModelGauge() {
        AtomicLong ref = new AtomicLong(0);
        PrometheusMetricsGroup.newModelGauge(PrometheusMetricsNameEnum.JOB_WAIT_DURATION_MAX, project, "test", ref, AtomicLong::get);
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertEquals(1, meters.size());
    }

    @Test
    public void testRemoveProjectMetrics() {
        Counter counter = Mockito.mock(Counter.class);
        PowerMockito.when(MetricsGroup.getCounter(PrometheusMetricsNameEnum.QUERY_TIMES.toMetricsName(),
                MetricsCategory.PROJECT, project, Collections.emptyMap())).thenReturn(counter);

        PrometheusMetricsGroup.removeProjectMetrics(project);

        PrometheusMetricsGroup.newMetricFromDropwizardCounterWithHostTag(PrometheusMetricsNameEnum.QUERY_TIMES, project);
        List<Meter> meters = meterRegistry.getMeters();

        Assert.assertEquals(1, meters.size());
        Assert.assertTrue(meters.stream().anyMatch(
                meter -> PrometheusMetricsNameEnum.QUERY_TIMES.getValue().equals(meter.getId().getName())));

        PrometheusMetricsGroup.removeProjectMetrics(project);
        meters = meterRegistry.getMeters();
        Assert.assertEquals(0, meters.size());

        thrown.expect(IllegalArgumentException.class);
        PrometheusMetricsGroup.removeProjectMetrics("");
    }

    @Test
    public void testRemoveModelMetrics() {
        String model = "test";

        PrometheusMetricsGroup.removeModelMetrics(project, model);

        AtomicLong ref = new AtomicLong(0);
        PrometheusMetricsGroup.newModelGauge(PrometheusMetricsNameEnum.MODEL_JOB_EXCEED_LAST_JOB_TIME_THRESHOLD, project, model, ref, AtomicLong::get);
        List<Meter> meters = meterRegistry.getMeters();

        Assert.assertEquals(1, meters.size());

        PrometheusMetricsGroup.removeModelMetrics(project, model);
        meters = meterRegistry.getMeters();
        Assert.assertEquals(0, meters.size());

        thrown.expect(IllegalArgumentException.class);
        PrometheusMetricsGroup.removeModelMetrics("", "");
    }
}