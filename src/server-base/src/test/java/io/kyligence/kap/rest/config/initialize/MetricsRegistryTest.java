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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.codahale.metrics.MetricFilter;

import io.kyligence.kap.common.metrics.MetricsController;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetrics;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ProjectService;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.val;
import lombok.var;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, MetricsGroup.class, UserGroupInformation.class, JdbcDataSource.class })
public class MetricsRegistryTest extends NLocalFileMetadataTestCase {

    private MeterRegistry meterRegistry;

    private String project = "default";

    Map<String, Long> totalStorageSizeMap;

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();

        totalStorageSizeMap = (Map<String, Long>) ReflectionTestUtils.getField(MetricsRegistry.class,
                "totalStorageSizeMap");
        totalStorageSizeMap.put(project, 1L);

        meterRegistry = new SimpleMeterRegistry();

        PowerMockito.mockStatic(SpringContext.class);
    }

    @Test
    public void testRefreshStorageVolumeInfo() {
        StorageVolumeInfoResponse response = Mockito.mock(StorageVolumeInfoResponse.class);
        Mockito.when(response.getTotalStorageSize()).thenReturn(2L);

        ProjectService projectService = PowerMockito.mock(ProjectService.class);
        Mockito.when(projectService.getStorageVolumeInfoResponse(project)).thenReturn(response);

        PowerMockito.when(SpringContext.getBean(ProjectService.class))
                .thenReturn(projectService);

        MetricsRegistry.refreshTotalStorageSize();
        Assert.assertEquals(totalStorageSizeMap.get(project), Long.valueOf(2));
    }

    @Test
    public void testRemoveProjectFromStorageSizeMap() {
        Assert.assertEquals(totalStorageSizeMap.size(), 1);
        MetricsRegistry.removeProjectFromStorageSizeMap(project);
        Assert.assertEquals(totalStorageSizeMap.size(), 0);
    }

    @Test
    public void testRegisterGlobalPrometheusMetrics() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        PowerMockito.mockStatic(JdbcDataSource.class);
        PowerMockito.when(JdbcDataSource.getDataSources()).thenReturn(Lists.newArrayList(dataSource));
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        MetricsRegistry.registerGlobalPrometheusMetrics();
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertEquals(5, meters.size());
        MetricsRegistry.registerGlobalMetrics(getTestConfig(), project);

        List<Meter> meterList = meterRegistry.getMeters().stream()
                .filter(e -> "idle".equals(e.getId().getTag(MetricsTag.STATE.getVal()))
                        || "active".equals(e.getId().getTag(MetricsTag.STATE.getVal()))).collect(Collectors.toList());
        Assert.assertEquals(2, meterList.size());
        Collection<Gauge> gauges = meterRegistry.find(PrometheusMetrics.JVM_DB_CONNECTIONS.getValue()).gauges();
        gauges.forEach(Gauge::value);
    }

    @Test
    public void testRegisterMicrometerProjectMetrics() {
        StorageVolumeInfoResponse response = Mockito.mock(StorageVolumeInfoResponse.class);
        Mockito.when(response.getTotalStorageSize()).thenReturn(2L);
        ProjectService projectService = PowerMockito.mock(ProjectService.class);
        Mockito.when(projectService.getStorageVolumeInfoResponse(project)).thenReturn(response);
        PowerMockito.when(SpringContext.getBean(ProjectService.class))
                .thenReturn(projectService);

        MetricsRegistry.registerProjectMetrics(getTestConfig(), project, "localhost");
        MetricsRegistry.registerHostMetrics("localhost");
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertEquals(0, meters.size());

        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setProject(project);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(executable);

        var result = MetricsController.getDefaultMetricRegistry()
                .getGauges(MetricFilter.contains(MetricsName.JOB_RUNNING_GAUGE.getVal()));
        Assert.assertEquals(1L, result.get(result.firstKey()).getValue());

        result = MetricsController.getDefaultMetricRegistry()
                .getGauges(MetricFilter.contains(MetricsName.JOB_ERROR_GAUGE.getVal()));
        Assert.assertEquals(0L, result.get(result.firstKey()).getValue());

        result = MetricsController.getDefaultMetricRegistry()
                .getGauges(MetricFilter.contains(MetricsName.JOB_PENDING_GAUGE.getVal()));
        Assert.assertEquals(1L, result.get(result.firstKey()).getValue());
    }

    @Test
    public void testDeletePrometheusProjectMetrics() {
        Assert.assertEquals(0, meterRegistry.getMeters().size());
        Counter counter = meterRegistry.counter(PrometheusMetrics.SPARK_TASKS.getValue(),
                Tags.of(MetricsTag.PROJECT.getVal(), "TEST"));
        counter.increment();

        Assert.assertNotEquals(0, meterRegistry.getMeters().size());
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        MetricsRegistry.deletePrometheusProjectMetrics("TEST");
        Assert.assertEquals(0, meterRegistry.getMeters().size());

        thrown.expect(IllegalArgumentException.class);
        MetricsRegistry.deletePrometheusProjectMetrics("");
    }

    @Test
    public void testRemovePrometheusModelMetrics() {
        Assert.assertEquals(0, meterRegistry.getMeters().size());
        Counter counter1 = meterRegistry.counter(PrometheusMetrics.MODEL_BUILD_DURATION.getValue(),
                Tags.of(MetricsTag.PROJECT.getVal(), "TEST", MetricsTag.MODEL.getVal(), "MODULE01"));
        Counter counter2 = meterRegistry.counter(PrometheusMetrics.MODEL_BUILD_DURATION.getValue(),
                Tags.of(MetricsTag.PROJECT.getVal(), "TEST", MetricsTag.MODEL.getVal(), "MODULE02"));
        counter1.increment();
        counter2.increment();

        Assert.assertEquals(2, meterRegistry.getMeters().size());
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        MetricsRegistry.removePrometheusModelMetrics("TEST", "MODULE01");

        Assert.assertEquals(1, meterRegistry.getMeters().size());
        List<Meter.Id> collect = meterRegistry.getMeters().stream().map(Meter::getId)
                .filter(id -> "TEST".equals(id.getTag(MetricsTag.PROJECT.getVal()))
                        && "MODULE01".equals(id.getTag(MetricsTag.MODEL.getVal())))
                .collect(Collectors.toList());

        Assert.assertEquals(0, collect.size());

        thrown.expect(IllegalArgumentException.class);
        MetricsRegistry.removePrometheusModelMetrics("", "");

        thrown.expect(IllegalArgumentException.class);
        MetricsRegistry.removePrometheusModelMetrics("project", "");
    }
}