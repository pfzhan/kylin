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

import static io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup.TAG_NAME_KYLIN_SERVER;
import static io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup.TAG_NAME_PROJECT;
import static io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup.generateProjectTags;
import static java.util.stream.Collectors.toSet;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.util.SpringContext;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetrics;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import io.kyligence.kap.metadata.cube.storage.StorageVolumeInfo;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.query.util.LoadCounter;
import io.kyligence.kap.query.util.LoadDesc;
import io.kyligence.kap.rest.response.SnapshotInfoResponse;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.SnapshotService;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsRegistry {
    private static final String GLOBAL = "global";

    private static Map<String, Long> totalStorageSizeMap = Maps.newHashMap();

    public static void refreshTotalStorageSize() {
        val projectService = SpringContext.getBean(ProjectService.class);
        totalStorageSizeMap.forEach((project, totalStorageSize) -> {
            val storageVolumeInfoResponse = projectService.getStorageVolumeInfoResponse(project);
            totalStorageSizeMap.put(project, storageVolumeInfoResponse.getTotalStorageSize());
        });
    }

    public static void removeProjectFromStorageSizeMap(String project) {
        totalStorageSizeMap.remove(project);
    }

    public static void registerGlobalMetrics(KylinConfig config, String host) {

        final NProjectManager projectManager = NProjectManager.getInstance(config);
        MetricsGroup.newGauge(MetricsName.PROJECT_GAUGE, MetricsCategory.GLOBAL, GLOBAL, () -> {
            List<ProjectInstance> list = projectManager.listAllProjects();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        final NKylinUserManager userManager = NKylinUserManager.getInstance(config);
        MetricsGroup.newGauge(MetricsName.USER_GAUGE, MetricsCategory.GLOBAL, GLOBAL, () -> {
            List<ManagedUser> list = userManager.list();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        Map<String, String> tags = MetricsGroup.getHostTagMap(host, GLOBAL);

        MetricsGroup.newCounter(MetricsName.STORAGE_CLEAN, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newCounter(MetricsName.STORAGE_CLEAN_DURATION, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newCounter(MetricsName.STORAGE_CLEAN_FAILED, MetricsCategory.GLOBAL, GLOBAL, tags);

        MetricsGroup.newCounter(MetricsName.METADATA_BACKUP, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newCounter(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newCounter(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newCounter(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newCounter(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL, tags);

        MetricsGroup.newCounter(MetricsName.TRANSACTION_RETRY_COUNTER, MetricsCategory.GLOBAL, GLOBAL, tags);
        MetricsGroup.newHistogram(MetricsName.TRANSACTION_LATENCY, MetricsCategory.GLOBAL, GLOBAL, tags);
    }

    public static void registerHostMetrics(String host) {
        MetricsGroup.newCounter(MetricsName.SPARDER_RESTART, MetricsCategory.HOST, host);
        MetricsGroup.newCounter(MetricsName.QUERY_HOST, MetricsCategory.HOST, host);
        MetricsGroup.newCounter(MetricsName.QUERY_SCAN_BYTES_HOST, MetricsCategory.HOST, host);
        MetricsGroup.newHistogram(MetricsName.QUERY_TIME_HOST, MetricsCategory.HOST, host);

        MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
        MetricsGroup.newGauge(MetricsName.HEAP_MAX, MetricsCategory.HOST, host,
                () -> mxBean.getHeapMemoryUsage().getMax());
        MetricsGroup.newGauge(MetricsName.HEAP_USED, MetricsCategory.HOST, host,
                () -> mxBean.getHeapMemoryUsage().getUsed());
        MetricsGroup.newGauge(MetricsName.HEAP_USAGE, MetricsCategory.HOST, host, () -> {
            final MemoryUsage usage = mxBean.getHeapMemoryUsage();
            return RatioGauge.Ratio.of(usage.getUsed(), usage.getMax()).getValue();
        });

        MetricsGroup.newMetricSet(MetricsName.JVM_GC, MetricsCategory.HOST, host, new GarbageCollectorMetricSet());
        MetricsGroup.newGauge(MetricsName.JVM_AVAILABLE_CPU, MetricsCategory.HOST, host,
                () -> Runtime.getRuntime().availableProcessors());
        MetricsGroup.newGauge(MetricsName.QUERY_LOAD, MetricsCategory.HOST, host, () -> {
            LoadDesc loadDesc = LoadCounter.getInstance().getLoadDesc();
            return loadDesc.getLoad();
        });

        MetricsGroup.newGauge(MetricsName.CPU_CORES, MetricsCategory.HOST, host, () -> {
            LoadDesc loadDesc = LoadCounter.getInstance().getLoadDesc();
            return loadDesc.getCoreNum();
        });

    }

    static void registerJobMetrics(KylinConfig config, String project) {
        final NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.JOB_ERROR_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<ExecutablePO> list = executableManager.getAllJobs();
            return list == null ? 0 : list.stream().filter(e -> ExecutableState.ERROR.name().equals(e.getOutput().getStatus())).count();
        });
        MetricsGroup.newGauge(MetricsName.JOB_RUNNING_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<ExecutablePO> list = executableManager.getAllJobs();
            return list == null ? 0 : list.stream().filter(e -> {
                String status = e.getOutput().getStatus();
                return ExecutableState.RUNNING.name().equals(status) || ExecutableState.READY.name().equals(status);
            }).count();
        });
        MetricsGroup.newGauge(MetricsName.JOB_PENDING_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<ExecutablePO> list = executableManager.getAllJobs();
            return list == null ? 0 : list.stream().filter(e -> ExecutableState.READY.name().equals(e.getOutput().getStatus())).count();
        });
    }

    static void registerStorageMetrics(String project) {
        val projectService = SpringContext.getBean(ProjectService.class);
        totalStorageSizeMap.put(project, projectService.getStorageVolumeInfoResponse(project).getTotalStorageSize());

        MetricsGroup.newGauge(MetricsName.PROJECT_STORAGE_SIZE, MetricsCategory.PROJECT, project,
                () -> totalStorageSizeMap.getOrDefault(project, 0L));

        MetricsGroup.newGauge(MetricsName.PROJECT_GARBAGE_SIZE, MetricsCategory.PROJECT, project, () -> {
            val collector = new ProjectStorageInfoCollector(Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE));
            StorageVolumeInfo storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(),
                    project);
            return storageVolumeInfo.getGarbageStorageSize();
        });
    }

    public static void registerProjectMetrics(KylinConfig config, String project, String host) {

        // for non-gauges
        MetricsGroup.registerProjectMetrics(project, host);

        //for gauges
        final NDataModelManager dataModelManager = NDataModelManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.MODEL_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<NDataModel> list = dataModelManager.listAllModels();
            return list == null ? 0 : list.size();
        });

        final NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.HEALTHY_MODEL_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<NDataModel> list = dataflowManager.listUnderliningDataModels();
            return list == null ? 0 : list.size();
        });

        registerStorageMetrics(project);
        registerJobMetrics(config, project);

        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.TABLE_GAUGE, MetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables();
            return list == null ? 0 : list.size();
        });
        MetricsGroup.newGauge(MetricsName.DB_GAUGE, MetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables();
            return list == null ? 0 : list.stream().map(TableDesc::getCaseSensitiveDatabase).collect(toSet()).size();
        });

        registerModelMetrics(config, project);
        registerJobStatisticsMetrics(project, host);
    }

    static void registerModelMetrics(KylinConfig config, String project) {
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        modelManager.listAllModels().forEach(model -> registerModelMetrics(project, model.getId(), model.getAlias()));
    }

    static void registerModelMetrics(String project, String modelId, String modelAlias) {
        ModelDropAddListener.onAdd(project, modelId, modelAlias);
    }

    public static void registerMicrometerGlobalMetrics() {
        PrometheusMetricsGroup.newMetrics();
    }

    public static void registerMicrometerProjectMetrics(KylinConfig config, String project, String host) {
        val counterMetrics = PrometheusMetrics.listProjectMetricsFromCounter();
        counterMetrics.forEach(metricName -> PrometheusMetricsGroup.newCounterFromDropwizard(metricName, project,
                MetricsGroup.getHostTagMap(AddressUtil.getZkLocalInstance(), project), generateProjectTags(project)));

        val metricsWithoutHostTag = PrometheusMetrics.listProjectMetricsFromGaugeWithoutHostTag();
        metricsWithoutHostTag.forEach(metricName -> PrometheusMetricsGroup.newGaugeFromDropwizard(metricName, project,
                Collections.emptyMap(), generateProjectTags(project)));

        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        registerMicrometerJobMetrics(executableManager, project, PrometheusMetrics.JOB_WAIT_DURATION_MAX,
                ExecutableState.READY);
        registerMicrometerJobMetrics(executableManager, project, PrometheusMetrics.JOB_RUNNING_DURATION_MAX,
                ExecutableState.RUNNING);
        registerMicrometerJobStatisticsMetrics(project, host);
        registerMicrometerStorageMetrics(project);
        registerMicrometerReportMetrics(project, host);
        registerMicrometerLoadMetrics();
    }

    static void registerMicrometerJobMetrics(NExecutableManager executableManager, String project,
            PrometheusMetrics metricsName, ExecutableState state) {
        ToDoubleFunction<AbstractExecutable> function;
        switch (metricsName) {
        case JOB_WAIT_DURATION_MAX:
            function = AbstractExecutable::getWaitTime;
            break;
        case JOB_RUNNING_DURATION_MAX:
            function = AbstractExecutable::getDuration;
            break;
        default:
            throw new IllegalStateException("Invalid metric name: " + metricsName.getValue());
        }

        PrometheusMetricsGroup.newProjectGauge(metricsName, project, executableManager, manager -> {
            List<AbstractExecutable> specificStatusJob = manager.getExecutablesByStatus(state);
            return specificStatusJob.stream().mapToDouble(function).max().orElse(0);
        });
    }

    private static void registerJobStatisticsMetrics(String project, String host) {
        List<JobTypeEnum> types = Stream.of(JobTypeEnum.values()).collect(Collectors.toList());
        Map<String, String> tags;
        for (JobTypeEnum type : types) {
            tags = Maps.newHashMap();
            tags.put(MetricsTag.HOST.getVal(), host);
            tags.put(MetricsTag.JOB_TYPE.getVal(), type.name());
            MetricsGroup.newCounter(MetricsName.JOB_COUNT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.SUCCESSFUL_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.ERROR_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.TERMINATED_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.JOB_COUNT_LT_5, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.JOB_COUNT_5_10, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.JOB_COUNT_10_30, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.JOB_COUNT_30_60, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.JOB_COUNT_GT_60, MetricsCategory.PROJECT, project, tags);
            MetricsGroup.newCounter(MetricsName.JOB_TOTAL_DURATION, MetricsCategory.PROJECT, project, tags);
        }
    }

    private static void registerMicrometerJobStatisticsMetrics(String project, String host) {
        List<JobTypeEnum> types = Stream.of(JobTypeEnum.values()).collect(Collectors.toList());
        Function<String, Tags> generateJobStatisticsTags = jobType -> {
            Tag hostTag = Tag.of(TAG_NAME_KYLIN_SERVER, host);
            Tag projectTag = Tag.of(TAG_NAME_PROJECT, project);
            Tag jobTypeTag = Tag.of("job_type", jobType);
            return Tags.of(hostTag, projectTag, jobTypeTag);
        };

        for (JobTypeEnum type : types) {
            Map<String, String> tags = Maps.newHashMap();
            tags.put(MetricsTag.HOST.getVal(), host);
            tags.put(MetricsTag.JOB_TYPE.getVal(), type.name());

            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT, MetricsName.JOB_COUNT, project,
                    tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.SUCCESSFUL_JOB_COUNT,
                    MetricsName.SUCCESSFUL_JOB_COUNT, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.ERROR_JOB_COUNT,
                    MetricsName.ERROR_JOB_COUNT, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.TERMINATED_JOB_COUNT,
                    MetricsName.TERMINATED_JOB_COUNT, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_LT_5,
                    MetricsName.JOB_COUNT_LT_5, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_5_10,
                    MetricsName.JOB_COUNT_5_10, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_10_30,
                    MetricsName.JOB_COUNT_10_30, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_30_60,
                    MetricsName.JOB_COUNT_30_60, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_GT_60,
                    MetricsName.JOB_COUNT_GT_60, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_TOTAL_DURATION,
                    MetricsName.JOB_TOTAL_DURATION, project, tags, generateJobStatisticsTags.apply(type.name()));
        }
    }

    private static void registerMicrometerStorageMetrics(String project) {
        Gauge<Long> gauge = MetricsGroup.getGauge(MetricsName.PROJECT_STORAGE_SIZE, MetricsCategory.PROJECT, project,
                Collections.emptyMap());
        PrometheusMetricsGroup.newProjectGaugeWithoutServerTag(PrometheusMetrics.STORAGE_SIZE, project, gauge,
                Gauge::getValue);

        gauge = MetricsGroup.getGauge(MetricsName.PROJECT_GARBAGE_SIZE, MetricsCategory.PROJECT, project,
                Collections.emptyMap());
        PrometheusMetricsGroup.newProjectGaugeWithoutServerTag(PrometheusMetrics.GARBAGE_SIZE, project, gauge,
                Gauge::getValue);
    }

    private static void registerMicrometerReportMetrics(String project, String host) {
        // Metrics without tags
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        PrometheusMetricsGroup.newGaugeIfAbsent(PrometheusMetrics.PROJECT_NUM_DAILY, projectManager,
                manager -> manager.listAllProjects().size(), Tags.empty());
        NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
        PrometheusMetricsGroup.newGaugeIfAbsent(PrometheusMetrics.USER_NUM_DAILY, userManager, um -> um.list().size(),
                Tags.empty());

        PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.QUERY_COUNT_DAILY, project,
                MetricsGroup.getHostTagMap(AddressUtil.getZkLocalInstance(), project), generateProjectTags(project));
        PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.FAILED_QUERY_COUNT_DAILY, project,
                MetricsGroup.getHostTagMap(AddressUtil.getZkLocalInstance(), project), generateProjectTags(project));
        PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.QUERY_TOTAL_DURATION_DAILY, project,
                MetricsGroup.getHostTagMap(AddressUtil.getZkLocalInstance(), project), generateProjectTags(project));
        PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.QUERY_COUNT_LT_1S_DAILY, project,
                MetricsGroup.getHostTagMap(AddressUtil.getZkLocalInstance(), project), generateProjectTags(project));
        PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.QUERY_COUNT_1S_3S_DAILY, project,
                MetricsGroup.getHostTagMap(AddressUtil.getZkLocalInstance(), project), generateProjectTags(project));

        Gauge<Long> gauge = MetricsGroup.getGauge(MetricsName.PROJECT_STORAGE_SIZE, MetricsCategory.PROJECT, project,
                Collections.emptyMap());
        PrometheusMetricsGroup.newProjectGauge(PrometheusMetrics.STORAGE_SIZE_DAILY, project, gauge, Gauge::getValue);

        gauge = MetricsGroup.getGauge(MetricsName.MODEL_GAUGE, MetricsCategory.PROJECT, project, Maps.newHashMap());
        PrometheusMetricsGroup.newProjectGauge(PrometheusMetrics.MODEL_NUM_DAILY, project, gauge, Gauge::getValue);
        NProjectManager pjManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        PrometheusMetricsGroup.newProjectGauge(PrometheusMetrics.SNAPSHOT_NUM_DAILY, project, pjManager, pm -> {
            if (pm.getProject(project).getConfig().isSnapshotManualManagementEnabled()) {
                SnapshotService snapshotService = SpringContext.getBean(SnapshotService.class);
                List<SnapshotInfoResponse> responses = snapshotService.getProjectSnapshots(project, null,
                        Collections.emptySet(), Collections.emptySet(), null, true);
                return responses.size();
            }
            return 0;
        });

        List<JobTypeEnum> types = Stream.of(JobTypeEnum.values()).collect(Collectors.toList());
        Function<String, Tags> generateJobStatisticsTags = jobType -> {
            Tag hostTag = Tag.of(TAG_NAME_KYLIN_SERVER, host);
            Tag projectTag = Tag.of(TAG_NAME_PROJECT, project);
            Tag jobTypeTag = Tag.of("job_type", jobType);
            return Tags.of(hostTag, projectTag, jobTypeTag);
        };
        for (JobTypeEnum type : types) {
            Map<String, String> tags = Maps.newHashMap();
            tags.put(MetricsTag.HOST.getVal(), host);
            tags.put(MetricsTag.JOB_TYPE.getVal(), type.name());

            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_DAILY, MetricsName.JOB_COUNT,
                    project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.SUCCESSFUL_JOB_COUNT_DAILY,
                    MetricsName.SUCCESSFUL_JOB_COUNT, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.ERROR_JOB_COUNT_DAILY,
                    MetricsName.ERROR_JOB_COUNT, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_TOTAL_DURATION_DAILY,
                    MetricsName.JOB_TOTAL_DURATION, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_LT_5_DAILY,
                    MetricsName.JOB_COUNT_LT_5, project, tags, generateJobStatisticsTags.apply(type.name()));
            PrometheusMetricsGroup.newCounterFromDropwizard(PrometheusMetrics.JOB_COUNT_5_10_DAILY,
                    MetricsName.JOB_COUNT_5_10, project, tags, generateJobStatisticsTags.apply(type.name()));
        }
    }

    private static void registerMicrometerLoadMetrics() {
        PrometheusMetricsGroup.newGaugeIfAbsent(PrometheusMetrics.SPARK_TASKS, LoadCounter.getInstance(),
                LoadCounter::getPendingTaskCount, //
                Tags.of("state", "pending", "instance", AddressUtil.getZkLocalInstance()));
        PrometheusMetricsGroup.newGaugeIfAbsent(PrometheusMetrics.SPARK_TASKS, LoadCounter.getInstance(),
                LoadCounter::getRunningTaskCount, //
                Tags.of("state", "running", "instance", AddressUtil.getZkLocalInstance()));
        PrometheusMetricsGroup.newGaugeIfAbsent(PrometheusMetrics.SPARK_TASK_UTILIZATION, LoadCounter.getInstance(),
                load -> load.getRunningTaskCount() * 1.0 / load.getSlotCount(), //
                Tags.of("instance", AddressUtil.getZkLocalInstance()));
    }
}
