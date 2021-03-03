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
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.util.SpringContext;

import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import io.kyligence.kap.metadata.cube.storage.StorageVolumeInfo;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.query.util.LoadCounter;
import io.kyligence.kap.query.util.LoadDesc;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;

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
        MetricsGroup.newGauge(MetricsName.QUERY_LOAD, MetricsCategory.HOST,
                host, () -> {
                    LoadDesc loadDesc = LoadCounter.getLoadDesc();
                    return loadDesc.getLoad();
                });

        MetricsGroup.newGauge(MetricsName.CPU_CORES, MetricsCategory.HOST,
                host, () -> {
                    LoadDesc loadDesc = LoadCounter.getLoadDesc();
                    return loadDesc.getCoreNum();
                });

    }

    static void registerJobMetrics(KylinConfig config, String project) {
        final NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.JOB_ERROR_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            return list == null ? 0 : list.stream().filter(e -> e.getStatus() == ExecutableState.ERROR).count();
        });
        MetricsGroup.newGauge(MetricsName.JOB_RUNNING_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            return list == null ? 0 : list.stream().filter(e -> e.getStatus().isProgressing()).count();
        });
        MetricsGroup.newGauge(MetricsName.JOB_PENDING_GAUGE, MetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            return list == null ? 0 : list.stream().filter(e -> e.getStatus() == ExecutableState.READY).count();
        });
    }

    static void registerStorageMetrics(String project) {
        val projectService = SpringContext.getBean(ProjectService.class);
        totalStorageSizeMap.put(project, projectService.getStorageVolumeInfoResponse(project).getTotalStorageSize());

        MetricsGroup.newGauge(MetricsName.PROJECT_STORAGE_SIZE, MetricsCategory.PROJECT, project, () ->
                totalStorageSizeMap.getOrDefault(project, 0L));

        MetricsGroup.newGauge(MetricsName.PROJECT_GARBAGE_SIZE, MetricsCategory.PROJECT, project, () -> {
            val collector = new ProjectStorageInfoCollector(Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE));
            StorageVolumeInfo storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(), project);
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

        final FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(config, project);
        MetricsGroup.newGauge(MetricsName.FQ_BLACKLIST, MetricsCategory.PROJECT, project, () -> {
            final Set<String> list = favoriteRuleManager.getBlacklistSqls();
            return list == null ? 0 : list.size();
        });

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
    }

    static void registerModelMetrics(KylinConfig config, String project) {
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        modelManager.listAllModels().forEach(model -> registerModelMetrics(project, model.getId(), model.getAlias()));
    }

    static void registerModelMetrics(String project, String modelId, String modelAlias) {
        ModelDropAddListener.onAdd(project, modelId, modelAlias);
    }
}
