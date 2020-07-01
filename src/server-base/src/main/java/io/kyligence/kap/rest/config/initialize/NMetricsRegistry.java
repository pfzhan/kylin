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

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;

public class NMetricsRegistry {
    private static final String GLOBAL = "global";

    protected static void registerGlobalMetrics(KylinConfig config) {

        final NProjectManager projectManager = NProjectManager.getInstance(config);
        NMetricsGroup.newGauge(NMetricsName.PROJECT_GAUGE, NMetricsCategory.GLOBAL, GLOBAL, () -> {
            List<ProjectInstance> list = projectManager.listAllProjects();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        final NKylinUserManager userManager = NKylinUserManager.getInstance(config);
        NMetricsGroup.newGauge(NMetricsName.USER_GAUGE, NMetricsCategory.GLOBAL, GLOBAL, () -> {
            List<ManagedUser> list = userManager.list();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        NMetricsGroup.newCounter(NMetricsName.STORAGE_CLEAN, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.STORAGE_CLEAN_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.STORAGE_CLEAN_FAILED, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.METADATA_BACKUP, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_BACKUP_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_BACKUP_FAILED, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_OPS_CRON, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.METADATA_OPS_CRON_SUCCESS, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.TRANSACTION_RETRY_COUNTER, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newHistogram(NMetricsName.TRANSACTION_LATENCY, NMetricsCategory.GLOBAL, GLOBAL);

        NMetricsGroup.newCounter(NMetricsName.BUILD_UNAVAILABLE_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
        NMetricsGroup.newCounter(NMetricsName.QUERY_UNAVAILABLE_DURATION, NMetricsCategory.GLOBAL, GLOBAL);
    }

    public static void registerHostMetrics(String host) {
        NMetricsGroup.newCounter(NMetricsName.SPARDER_RESTART, NMetricsCategory.HOST, host);
        NMetricsGroup.newCounter(NMetricsName.QUERY_HOST, NMetricsCategory.HOST, host);
        NMetricsGroup.newCounter(NMetricsName.QUERY_SCAN_BYTES_HOST, NMetricsCategory.HOST, host);
        NMetricsGroup.newHistogram(NMetricsName.QUERY_TIME_HOST, NMetricsCategory.HOST, host);

        MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
        NMetricsGroup.newGauge(NMetricsName.HEAP_MAX, NMetricsCategory.HOST, host,
                () -> mxBean.getHeapMemoryUsage().getMax());
        NMetricsGroup.newGauge(NMetricsName.HEAP_USED, NMetricsCategory.HOST, host,
                () -> mxBean.getHeapMemoryUsage().getUsed());
        NMetricsGroup.newGauge(NMetricsName.HEAP_USAGE, NMetricsCategory.HOST, host, () -> {
            final MemoryUsage usage = mxBean.getHeapMemoryUsage();
            return RatioGauge.Ratio.of(usage.getUsed(), usage.getMax()).getValue();
        });

        NMetricsGroup.newMetricSet(NMetricsName.JVM_GC, NMetricsCategory.HOST, host, new GarbageCollectorMetricSet());
        NMetricsGroup.newGauge(NMetricsName.JVM_AVAILABLE_CPU, NMetricsCategory.HOST, host,
                () -> Runtime.getRuntime().availableProcessors());

    }

    static void registerJobMetrics(KylinConfig config, String project) {
        final NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.JOB_ERROR_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            return list == null ? 0 : list.stream().filter(e -> e.getStatus() == ExecutableState.ERROR).count();
        });
        NMetricsGroup.newGauge(NMetricsName.JOB_RUNNING_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            return list == null ? 0 : list.stream().filter(e -> e.getStatus().isProgressing()).count();
        });
        NMetricsGroup.newGauge(NMetricsName.JOB_PENDING_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            return list == null ? 0 : list.stream().filter(e -> e.getStatus() == ExecutableState.READY).count();
        });
    }

    static void registerStorageMetrics(String project) {
        val projectService = SpringContext.getBean(ProjectService.class);
        NMetricsGroup.newGauge(NMetricsName.PROJECT_STORAGE_SIZE, NMetricsCategory.PROJECT, project, () -> {
            StorageVolumeInfoResponse resp = projectService.getStorageVolumeInfoResponse(project);
            return resp == null ? 0L : resp.getTotalStorageSize();
        });
        NMetricsGroup.newGauge(NMetricsName.PROJECT_GARBAGE_SIZE, NMetricsCategory.PROJECT, project, () -> {
            StorageVolumeInfoResponse resp = projectService.getStorageVolumeInfoResponse(project);
            return resp == null ? 0L : resp.getGarbageStorageSize();
        });
    }

    public static void registerProjectMetrics(KylinConfig config, String project) {

        // for non-gauges
        NMetricsGroup.registerProjectMetrics(project);

        //for gauges
        final EventDao eventDao = EventDao.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.EVENT_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<Event> list = eventDao.getEvents();
            return list == null ? 0 : list.size();
        });

        final NDataModelManager dataModelManager = NDataModelManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.MODEL_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<NDataModel> list = dataModelManager.listAllModels();
            return list == null ? 0 : list.size();
        });

        final NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.HEALTHY_MODEL_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<NDataModel> list = dataflowManager.listUnderliningDataModels();
            return list == null ? 0 : list.size();
        });

        registerStorageMetrics(project);

        registerJobMetrics(config, project);

        final FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.FQ_BLACKLIST, NMetricsCategory.PROJECT, project, () -> {
            final Set<String> list = favoriteRuleManager.getBlacklistSqls();
            return list == null ? 0 : list.size();
        });

        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.TABLE_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables();
            return list == null ? 0 : list.size();
        });
        NMetricsGroup.newGauge(NMetricsName.DB_GAUGE, NMetricsCategory.PROJECT, project, () -> {
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
