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

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class BootstrapCommand implements Runnable {

    @Autowired
    ProjectService projectService;

    @Override
    public void run() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kylinConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            initProject(kylinConfig, project.getName());
        }

        for (val scheduler : NDefaultScheduler.listAllSchedulers()) {
            val project = scheduler.getProject();
            if (projectManager.getProject(scheduler.getProject()) == null) {
                EventOrchestratorManager.getInstance(kylinConfig).shutdownByProject(project);
                NFavoriteScheduler.shutdownByProject(project);
                NDefaultScheduler.shutdownByProject(project);
            }
        }
    }

    void initProject(KylinConfig config, String project) {
        val leaderInitiator = LeaderInitiator.getInstance(config);
        if (!leaderInitiator.isLeader()) {
            return;
        }
        if (NFavoriteScheduler.getInstance(project).hasStarted()) {
            return;
        }
        UnitOfWork.doInTransactionWithRetry(() -> {
            EventOrchestratorManager.getInstance(config).addProject(project);
            NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
            scheduler.init(new JobEngineConfig(config), new MockJobLock());
            if (!scheduler.hasStarted()) {
                throw new RuntimeException("Scheduler for " + project + " has not been started");
            }

            NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project);
            favoriteScheduler.init();

            if (!favoriteScheduler.hasStarted()) {
                throw new RuntimeException("Auto favorite scheduler for " + project + " has not been started");
            }
            return 0;
        }, project, 1);

        registerProjectMetrics(config, project);
        log.info("init project {} finished", project);
    }

    void registerProjectMetrics(KylinConfig config, String project) {

        // for non-gauges
        NMetricsGroup.registerProjectMetrics(project);

        //for gauges
        final EventDao eventDao = EventDao.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.EVENT_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<Event> list = eventDao.getEvents();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        final NDataModelManager dataModelManager = NDataModelManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.MODEL_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<NDataModel> list = dataModelManager.listAllModels();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        final NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.HEALTHY_MODEL_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<NDataModel> list = dataflowManager.listUnderliningDataModels();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        NMetricsGroup.newGauge(NMetricsName.PROJECT_STORAGE_SIZE, NMetricsCategory.PROJECT, project, () -> {
            StorageVolumeInfoResponse resp = projectService.getStorageVolumeInfoResponse(project);
            if (resp == null) {
                return 0L;
            }
            return resp.getTotalStorageSize();
        });
        NMetricsGroup.newGauge(NMetricsName.PROJECT_GARBAGE_SIZE, NMetricsCategory.PROJECT, project, () -> {
            StorageVolumeInfoResponse resp = projectService.getStorageVolumeInfoResponse(project);
            if (resp == null) {
                return 0L;
            }
            return resp.getGarbageStorageSize();
        });

        final NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.JOB_ERROR_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(e -> e.getStatus() == ExecutableState.ERROR).count();
        });
        NMetricsGroup.newGauge(NMetricsName.JOB_RUNNING_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            List<AbstractExecutable> list = executableManager.getAllExecutables();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(e -> e.getStatus().isProgressing()).count();
        });

        final FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.FQ_TO_BE_ACCELERATED, NMetricsCategory.PROJECT, project, () -> {
            final List<FavoriteQuery> list = favoriteQueryManager.getAll();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(fq -> fq.getStatus() == FavoriteQueryStatusEnum.TO_BE_ACCELERATED).count();
        });
        NMetricsGroup.newGauge(NMetricsName.FQ_ACCELERATED, NMetricsCategory.PROJECT, project, () -> {
            final List<FavoriteQuery> list = favoriteQueryManager.getAll();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(fq -> fq.getStatus() == FavoriteQueryStatusEnum.ACCELERATED).count();
        });
        NMetricsGroup.newGauge(NMetricsName.FQ_FAILED, NMetricsCategory.PROJECT, project, () -> {
            final List<FavoriteQuery> list = favoriteQueryManager.getAll();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(fq -> fq.getStatus() == FavoriteQueryStatusEnum.FAILED).count();
        });
        NMetricsGroup.newGauge(NMetricsName.FQ_ACCELERATING, NMetricsCategory.PROJECT, project, () -> {
            final List<FavoriteQuery> list = favoriteQueryManager.getAll();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(fq -> fq.getStatus() == FavoriteQueryStatusEnum.ACCELERATING).count();
        });
        NMetricsGroup.newGauge(NMetricsName.FQ_PENDING, NMetricsCategory.PROJECT, project, () -> {
            final List<FavoriteQuery> list = favoriteQueryManager.getAll();
            if (list == null) {
                return 0;
            }
            return list.stream().filter(fq -> fq.getStatus() == FavoriteQueryStatusEnum.PENDING).count();
        });

        final FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.FQ_BLACKLIST, NMetricsCategory.PROJECT, project, () -> {
            final Set<String> list = favoriteRuleManager.getBlacklistSqls();
            if (list == null) {
                return 0;
            }
            return list.size();
        });

        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, project);
        NMetricsGroup.newGauge(NMetricsName.TABLE_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables();
            if (list == null) {
                return 0;
            }
            return list.size();
        });
        NMetricsGroup.newGauge(NMetricsName.DB_GAUGE, NMetricsCategory.PROJECT, project, () -> {
            final List<TableDesc> list = tableMetadataManager.listAllTables();
            if (list == null) {
                return 0;
            }
            return list.stream().map(t -> t.getCaseSensitiveDatabase()).collect(toSet()).size();
        });
    }
}