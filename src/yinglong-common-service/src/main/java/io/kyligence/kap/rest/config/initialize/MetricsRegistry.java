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

import static java.util.stream.Collectors.toSet;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.event.ModelAddEvent;
import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetrics;
import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.job.manager.ExecutableManager;
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
import io.kyligence.kap.rest.service.ProjectService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsRegistry {
    private static final String GLOBAL = "global";

    private static Map<String, Long> totalStorageSizeMap = Maps.newHashMap();

    private static final Logger logger = LoggerFactory.getLogger(MetricsRegistry.class);

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

    public static void registerGlobalPrometheusMetrics() {
        MeterRegistry meterRegistry = SpringContext.getBean(MeterRegistry.class);
        for (String state : Lists.newArrayList("idle", "active")) {
            JdbcDataSource.getDataSources().stream()
                    .collect(Collectors.groupingBy(ds -> ((BasicDataSource) ds).getDriverClassName()))
                    .forEach((driver, sources) -> Gauge
                            .builder(PrometheusMetrics.JVM_DB_CONNECTIONS.getValue(), sources, dataSources -> {
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
                            }).tags(MetricsTag.STATE.getVal(), state, MetricsTag.POOL.getVal(), "dbcp2",
                                    MetricsTag.TYPE.getVal(), driver)
                            .strongReference(true).register(meterRegistry));
        }

        Gauge.builder(PrometheusMetrics.SPARDER_UP.getValue(), () -> SparderEnv.isSparkAvailable() ? 1 : 0)
                .strongReference(true).register(meterRegistry);

        Gauge.builder(PrometheusMetrics.SPARK_TASKS.getValue(), LoadCounter.getInstance(),
                e -> SparderEnv.isSparkAvailable() ? e.getPendingTaskCount() : 0)
                .tags(MetricsTag.STATE.getVal(), MetricsTag.PENDING.getVal()).strongReference(true)
                .register(meterRegistry);
        Gauge.builder(PrometheusMetrics.SPARK_TASKS.getValue(), LoadCounter.getInstance(),
                e -> SparderEnv.isSparkAvailable() ? e.getRunningTaskCount() : 0)
                .tags(MetricsTag.STATE.getVal(), MetricsTag.RUNNING.getVal()).strongReference(true)
                .register(meterRegistry);
        Gauge.builder(PrometheusMetrics.SPARK_TASK_UTILIZATION.getValue(), LoadCounter.getInstance(),
                e -> SparderEnv.isSparkAvailable() ? e.getRunningTaskCount() * 1.0 / e.getSlotCount() : 0)
                .strongReference(true).register(meterRegistry);
    }
    // TODO need to be rewritten
    public static void registerProjectPrometheusMetrics(KylinConfig kylinConfig, String project) {
//        if (!kylinConfig.isPrometheusMetricsEnabled()) {
//            return;
//        }
//        MeterRegistry meterRegistry = SpringContext.getBean(MeterRegistry.class);
//        Tags projectTag = Tags.of(MetricsTag.PROJECT.getVal(), project);
//        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
//        Gauge.builder(PrometheusMetrics.JOB_COUNTS.getValue(),
//                () -> Objects.isNull(scheduler.getContext()) ? 0
//                        : scheduler.getContext().getRunningJobs().values().stream()
//                                .filter(job -> ExecutableState.RUNNING.equals(job.getOutput().getState())).count())
//                .tags(projectTag).tags(MetricsTag.STATE.getVal(), MetricsTag.RUNNING.getVal()).register(meterRegistry);
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
        final ExecutableManager executableManager = ExecutableManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.JOB_ERROR_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<ExecutablePO> list = executableManager.getAllJobs();
            return list == null ? 0 : list.stream().filter(e -> ExecutableState.ERROR.name().equals(e.getOutput().getStatus())).count();
        });
        MetricsGroup.newGauge(MetricsName.JOB_RUNNING_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<ExecutablePO> list = executableManager.getAllJobs();
            return list == null ? 0 : list.stream().filter(e -> {
                String status = e.getOutput().getStatus();
                return ExecutableState.RUNNING.name().equals(status) || ExecutableState.READY.name().equals(status)
                        || ExecutableState.PENDING.name().equals(status);
            }).count();
        });
        MetricsGroup.newGauge(MetricsName.JOB_PENDING_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<ExecutablePO> list = executableManager.getAllJobs();
            return list == null ? 0
                    : list.stream().filter(e -> ExecutableState.READY.name().equals(e.getOutput().getStatus())
                            || ExecutableState.PENDING.name().equals(e.getOutput().getStatus())).count();
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
            List<NDataModel> list = dataflowManager.listUnderliningDataModels()
                    .stream().filter(NDataModelManager::isModelAccessible).collect(Collectors.toList());
            return list == null ? 0 : list.size();
        });

        registerStorageMetrics(project);
        registerJobMetrics(config, project);

        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.TABLE_GAUGE, MetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables().stream()
                    .filter(NTableMetadataManager::isTableAccessible).collect(Collectors.toList());
            return list == null ? 0 : list.size();
        });
        MetricsGroup.newGauge(MetricsName.DB_GAUGE, MetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables();
            return list == null ? 0 : list.stream().filter(NTableMetadataManager::isTableAccessible)
                    .map(TableDesc::getCaseSensitiveDatabase).collect(toSet()).size();
        });

        registerModelMetrics(config, project);
        registerJobStatisticsMetrics(project, host);
    }

    static void registerModelMetrics(KylinConfig config, String project) {
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        modelManager.listAllModels().stream().filter(NDataModelManager::isModelAccessible).forEach(model ->
                registerModelMetrics(project, model.getId(), model.getAlias()));
    }

    static void registerModelMetrics(String project, String modelId, String modelAlias) {
        EventBusFactory.getInstance().postSync(
                new ModelAddEvent(project, modelId, modelAlias));
    }

    public static void deletePrometheusProjectMetrics(String project) {
        if (StringUtils.isEmpty(project)) {
            throw new IllegalArgumentException("Remove prometheus project metrics, project shouldn't be empty.");
        }

        MeterRegistry meterRegistry = SpringContext.getBean(MeterRegistry.class);
        meterRegistry.getMeters().stream().map(Meter::getId)
                .filter(id -> project.equals(id.getTag(MetricsTag.PROJECT.getVal()))).forEach(meterRegistry::remove);

        logger.info("Remove project prometheus metrics for {} success.", project);
    }

    public static void removePrometheusModelMetrics(String project, String modelName) {
        if (StringUtils.isBlank(project) || StringUtils.isBlank(modelName)) {
            throw new IllegalArgumentException(
                    "Remove prometheus model metrics, project or modelName shouldn't be empty.");
        }

        Set<PrometheusMetrics> modelMetrics = PrometheusMetrics.listModelMetrics();

        modelMetrics.forEach(metricName -> doRemoveMetric(metricName,
                Tags.of(MetricsTag.PROJECT.getVal(), project, MetricsTag.MODEL.getVal(), modelName)));
    }

    private static void doRemoveMetric(PrometheusMetrics metricName, Tags tags) {
        MeterRegistry meterRegistry = SpringContext.getBean(MeterRegistry.class);
        Meter result = meterRegistry
                .remove(new Meter.Id(metricName.getValue(), tags, null, null, Meter.Type.DISTRIBUTION_SUMMARY));
        if (Objects.isNull(result)) {
            logger.warn("Remove prometheus metric failed, metric name: {}, tags: {}", metricName.getValue(), tags);
        }
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

}
